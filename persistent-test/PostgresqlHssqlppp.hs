{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module PostgresqlHssqlppp
 (  persistL
  , persistU
  , getSqlCode
 ) where

import Database.Persist.Sql hiding (Statement)
import Database.Persist.Postgresql hiding (Statement)
import Control.Monad.IO.Class (MonadIO (..))
import Data.IORef

import Language.Haskell.TH.Quote
import Database.Persist.TH (mkPersist, mkMigrate, share, sqlSettings, persistLowerCase, persistUpperCase, persistWith)
import Database.Persist.Quasi

import Database.HsSqlPpp.Quote
import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Pretty
import Database.HsSqlPpp.Annotation
import Database.HsSqlPpp.Parser

import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import Data.Text (Text)

import Data.Conduit
import Data.Maybe
import Data.List
import Data.Char

import Debug.Trace

-- | The three types of trigger supported by PostgreSQL
data PostgreSqlTriggerType = BEFORE | AFTER | INSTEADOF
  deriving (Eq,Read)

instance Show PostgreSqlTriggerType where
  show BEFORE    = "BEFORE"
  show AFTER     = "AFTER"
  show INSTEADOF = "INSTEAD OF"

-- | Does not support "UPDATE OF" syntax
data PostgreSqlTriggerEvent =  INSERT | UPDATE | DELETE | TRUNCATE | OR
  deriving (Eq,Show,Read)

-- | Determines if given trigger name exists in passed SQL
isSqlTrigger :: [LT.Text] -> String -> Bool
isSqlTrigger sql name = any (== Just (map toLower name, map toLower "trigger"))
                      $ fmap getSqlFuncAttrs
                      $ fmap parsePostgreSQL sql

-- | Validate trigger entry in extras
validateTriggers :: [LT.Text]
                -> Maybe [[Text]]
                -> Bool
validateTriggers _ Nothing = False
validateTriggers sql (Just ps) = all id $ map (validateTrigger sql) ps
  where validateTrigger sql params = let
          triggerFunc = params !! 0
          triggerType = params !! 1
          triggerEvns = drop 2 params
          in    (length params >= 3)
             && ( not . null
                $ (reads (T.unpack triggerType) :: [(PostgreSqlTriggerType,String)]))
             && ( all (\te ->  not . null
                $ (reads (T.unpack te) :: [(PostgreSqlTriggerEvent,String)]))
                     triggerEvns)
             && (isSqlTrigger sql $ T.unpack triggerFunc)

-- | Validate extras: only supports Triggers for now.
valExtras :: ExtrasValidate LT.Text
valExtras sql extras = let
    trigs= Map.lookup "Triggers" extras
    test =  isJust    (trigs) && validateTriggers sql trigs
         || isNothing (trigs)
    in if test
          then extras
          else error $ "Invalid SQL Triggers:" ++ (show $ concat . fromJust $ trigs)

-- | Lower Case quasiquote with custom SQL
persistL :: [LT.Text] -> QuasiQuoter
persistL extras =
  persistWith
    lowerCaseSettings { validateExtras = Just (valExtras extras) }

-- | Uppper Case quasiquote with custom SQL
persistU :: [LT.Text] -> QuasiQuoter
persistU extras =
  persistWith
    upperCaseSettings { validateExtras = Just (valExtras extras) }

-- | Given a SQL statement, determines if it is a function, returning its name
-- and SQL.
getSqlFuncCode :: Statement -> Maybe (String,Statement)
getSqlFuncCode sql = case sql of
   (CreateFunction _ (Name _ ns) _ _ _ _ (PlpgsqlFnBody _ cd) _)
      -> if null ns
          then Nothing
          else Just (ncStr $ last ns, sql)
   _  -> Nothing


-- | Get SQL Function name and type
getSqlFuncAttrs :: Statement -> Maybe (String,String)
getSqlFuncAttrs sql = case sql of
   (CreateFunction _ (Name _ fns) _ (SimpleTypeName _ (Name _ tns)) _ _ _ _)
      -> if null fns || null tns
            then Nothing
            else Just (ncStr $ last fns,ncStr $ last tns)
   _  -> Nothing


-- | Get SQL code for extras.
-- Currently only supports Triggers
getSqlCode :: GetExtrasSql LT.Text
getSqlCode triggers tn (entry,line) =
    T.concat $ map getSqlCode' line
  where getSqlCode' values = let
          result = case entry of
            "Triggers" -> let
              fn    = head values
              tevns = map (T.unpack) $ drop 2 values
              -- trigger function
              tf  = maybe ""
                    ( LT.unpack
                    . printStatements (PrettyPrintFlags PostgreSQLDialect)
                    . replicate 1
                    . snd)
                  $ find (\(n,_) -> n == (T.unpack . T.toLower) fn)
                  $ catMaybes
                  $ map (getSqlFuncCode . parsePostgreSQL) triggers
              -- trigger events
              tevns' = concat $ map readEvent tevns
              ct   = createAfterTriggerOnRow'
                        (T.unpack fn)
                        tevns'
                        (T.unpack tn)
                        (T.unpack fn)
              in tf ++ ct
            _ -> error "Only PostgreSQL triggers supported for the moment"
          in if length values >= 3
               then T.pack result
               else error "Invalid Trigger Specification"

          where readEvent e = let
                  event = reads e :: [(PostgreSqlTriggerEvent,String)]
                  in  if not . null $ event
                       then fmap fst event
                       else error $ "Invalid Trigger parameter:" ++ show e

-- | Parse text using HsSqlPpp
parsePostgreSQL :: LT.Text -> Statement
parsePostgreSQL sql = let
  res = parsePlpgsql
    (ParseFlags PostgreSQLDialect)
    __FILE__
    Nothing
    sql
  in either
      (\pe -> error $ show pe)
      (\s  -> if null s
                then error $ "Valid SQL returned no statements"
                else head s)
      res

------------------------------------------------------------------------------
-- Create Triggers -----------------------------------------------------------
------------------------------------------------------------------------------

createAfterTriggerOnRow' name events table fn =
    LT.unpack
  . printStatements (PrettyPrintFlags PostgreSQLDialect)
  . replicate 1
  $ createAfterTriggerOnRow name events table fn


convertTriggerEvents :: [PostgreSqlTriggerEvent] -> [TriggerEvent]
convertTriggerEvents tes = let
  conv e = case e of
    UPDATE -> Just TUpdate
    INSERT -> Just TInsert
    DELETE -> Just TDelete
    TRUNCATE->Just TDelete
    OR     -> Nothing
  in mapMaybe conv tes

createAfterTriggerOnRow
  :: String
  -> [PostgreSqlTriggerEvent]
  -> String
  -> String
  -> Statement
createAfterTriggerOnRow name' events' table' fn' = let
  annot  = Annotation (Just (__FILE__,__LINE__,0)) Nothing  []  Nothing  []
  name   = Nmc name'
  events = convertTriggerEvents events'
  table  = Name annot [Nmc table']
  fn     = Name annot [Nmc fn']
  in CreateTrigger
      annot
      name
      TriggerAfter
      events
      table
      EachRow
      fn []


