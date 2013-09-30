{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module PostgresqlHssqlppp
 (  persistW
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

data PostgreSqlTriggerType = BEFORE | AFTER | INSTEADOF
  deriving (Eq,Read)

instance Show PostgreSqlTriggerType where
  show BEFORE    = "BEFORE"
  show AFTER     = "AFTER"
  show INSTEADOF = "INSTEAD OF"

data PostgreSqlTriggerEvent =  INSERT | UPDATE | DELETE | TRUNCATE | OR
  deriving (Eq,Show,Read)


isSqlTrigger :: [LT.Text] -> String -> Bool
isSqlTrigger sql name = any (== Just (map toLower name, map toLower "trigger"))
                      $ fmap getSqlFuncAttrs
                      $ fmap parsePostgreSQL sql

validateTrigger :: [LT.Text]
                -> Maybe [[Text]]
                -> Bool
validateTrigger _ Nothing = False
validateTrigger sql (Just ps) = let
  params = concat ps
  triggerFunc = params !! 0
  triggerType = params !! 1
  triggerEvns = drop 2 params
  in    (length params >= 3)
     && (not . null $ (reads (T.unpack triggerType) :: [(PostgreSqlTriggerType,String)]))
     && (all (\te ->  not . null
                   $ (reads (T.unpack te) :: [(PostgreSqlTriggerEvent,String)]))
             triggerEvns)
     && (isSqlTrigger sql $ T.unpack triggerFunc)

valExtras :: ExtrasValidate LT.Text
valExtras sql extras = let
    trigs= Map.lookup "Triggers" extras
    test =  isJust    (trigs) && validateTrigger sql trigs
         || isNothing (trigs)
    in if test
          then extras
          else error $ "Invalid SQL Triggers:" ++ (show $ concat . fromJust $ trigs)

persistW :: [LT.Text] -> QuasiQuoter
persistW extras =
  persistWith
    lowerCaseSettings { validateExtras = Just (valExtras extras) }


getSqlFuncCode :: Statement -> Maybe (String,Statement)
getSqlFuncCode sql = case sql of
   (CreateFunction _ (Name _ ns) _ _ _ _ (PlpgsqlFnBody _ cd) _)
      -> if null ns
          then Nothing
          else Just (ncStr $ last ns, sql)
   _  -> Nothing


getSqlFuncAttrs :: Statement -> Maybe (String,String)
getSqlFuncAttrs sql = case sql of
   (CreateFunction _ (Name _ fns) _ (SimpleTypeName _ (Name _ tns)) _ _ _ _)
      -> if null fns || null tns
            then Nothing
            else Just (ncStr $ last fns,ncStr $ last tns)
   _  -> Nothing


-- I need to pass more information besides a string with the name of the function
-- If I want to construct a custom trigger, I need the
-- * trigger name
-- * trigger events
-- * table name
-- * function name
getSqlCode :: GetExtrasSql LT.Text
getSqlCode triggers tn (entry,line) = let
  values = concat line
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
      ct   = createAfterTriggerOnRow' (T.unpack fn) tevns' (T.unpack tn) (T.unpack fn)
      in tf ++ ct
    _ -> error "Only PostgreSQL triggers supported for the moment"
  in if length values >= 3
       then T.pack result
       else error "Invalid Trigger Specification"

  where readEvent e = let
          event = reads e :: [(PostgreSqlTriggerEvent,String)]
          in if not . null $ event
               then fmap fst event
               else error "Invalid Trigger parameter"

createAfterTriggerOnRow' name events table fn =
    LT.unpack
  . printStatements (PrettyPrintFlags PostgreSQLDialect)
  . replicate 1
  $ createAfterTriggerOnRow name events table fn

------------------------------------------------------------------------------
-- Parse SQL ----- -----------------------------------------------------------
------------------------------------------------------------------------------
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


