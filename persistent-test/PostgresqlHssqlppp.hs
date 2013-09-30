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

import Language.Haskell.TH.Quote (QuasiQuoter)

import Database.Persist.Postgresql hiding (Statement)
import Database.Persist.TH (persistWith)
import Database.Persist.Quasi

import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Pretty
import Database.HsSqlPpp.Annotation
import Database.HsSqlPpp.Parser

import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import Data.Text (Text)

import Data.Maybe
import Data.List
import Data.Char

import Debug.Trace

-- | The two types of trigger supported by PostgreSQL tables.
-- Views also support 'Instead Of', but views are not supported ATM.
data PostgreSqlTriggerType = BEFORE | AFTER
  deriving (Eq,Read)

instance Show PostgreSqlTriggerType where
  show BEFORE    = "BEFORE"
  show AFTER     = "AFTER"

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
  where validateTrigger sql' params = let
          triggerFunc = params !! 0
          triggerType = params !! 1
          triggerEvns = drop 2 params
          t1 = length params >= 3 || error ("Insufficient parameters" ++ show params)
          t2 = ( not . null
               $ (reads (T.unpack triggerType) :: [(PostgreSqlTriggerType,String)]))
             || error ("Invalid trigger type:" ++ show triggerType)
          t3 = ( all (\te ->  not . null
                     $ (reads (T.unpack te) :: [(PostgreSqlTriggerEvent,String)]))
                     $ triggerEvns)
             || error ("Invalid trigger events:" ++ show triggerEvns)
          t4 = (isSqlTrigger sql' $ T.unpack triggerFunc)
             || error ("There is no (Postgre)SQL function with name:" ++ show triggerFunc)
          in all id [t1,t2,t3,t4]

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
   (CreateFunction _ (Name _ ns) _ _ _ _ (PlpgsqlFnBody _ _) _)
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
              fn    = values !! 0
              when  = T.unpack $ values !! 1
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
              ct   = createTriggerOnRow'
                        (T.unpack fn)
                        (readWhen when)
                        (concat $ map readEvent tevns)
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
                       else error $ "Invalid Trigger Event:" ++ show e
                readWhen w = let
                  when = reads w :: [(PostgreSqlTriggerType,String)]
                  in  if not . null $ when
                        then head $ fmap fst when
                        else error $ "Invalid Trigger Type:" ++ show w

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
createTriggerOnRow' :: String
                    -> PostgreSqlTriggerType
                    -> [PostgreSqlTriggerEvent]
                    -> String
                    -> String
                    -> String
createTriggerOnRow' name when events table fn =
    LT.unpack
  . printStatements (PrettyPrintFlags PostgreSQLDialect)
  . replicate 1
  $ createTriggerOnRow name when events table fn


convertTriggerEvents :: [PostgreSqlTriggerEvent] -> [TriggerEvent]
convertTriggerEvents tes = let
  conv e = case e of
    UPDATE -> Just TUpdate
    INSERT -> Just TInsert
    DELETE -> Just TDelete
    TRUNCATE->Just TDelete
    OR     -> Nothing
  in mapMaybe conv tes

convertTriggerType :: PostgreSqlTriggerType -> TriggerWhen
convertTriggerType tt = case tt of
  BEFORE    -> TriggerBefore
  AFTER     -> TriggerAfter

createTriggerOnRow
  :: String
  -> PostgreSqlTriggerType
  -> [PostgreSqlTriggerEvent]
  -> String
  -> String
  -> Statement
createTriggerOnRow name' typ' events' table' fn' = let
  annot  = Annotation (Just (__FILE__,__LINE__,0)) Nothing  []  Nothing  []
  name   = Nmc name'
  typ    = convertTriggerType typ'
  events = convertTriggerEvents events'
  table  = Name annot [Nmc table']
  fn     = Name annot [Nmc fn']
  in CreateTrigger
      annot
      name
      typ
      events
      table
      EachRow
      fn []


