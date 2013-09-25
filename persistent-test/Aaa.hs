{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Aaa
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

import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import Data.Text (Text)

import Data.Conduit
import Data.Maybe
import Data.List
import Data.Char

import Triggers

data PostgreSqlTriggerType = BEFORE | AFTER | INSTEADOF
  deriving (Eq,Read)

instance Show PostgreSqlTriggerType where
  show BEFORE    = "BEFORE"
  show AFTER     = "AFTER"
  show INSTEADOF = "INSTEAD OF"

data PostgreSqlTriggerEvent =  INSERT | UPDATE | DELETE | TRUNCATE | OR
  deriving (Eq,Show,Read)


isSqlTrigger :: String -> Bool
isSqlTrigger name = any (== Just (map toLower name, map toLower "trigger"))
                  $ fmap (getSqlFuncAttrs) triggers

validateTrigger :: Maybe [[Text]]
                -> Bool
validateTrigger Nothing = False
validateTrigger (Just ps) = let
  params = concat ps
  triggerFunc = params !! 0
  triggerType = params !! 1
  triggerEvns = drop 2 params
  in    (length params >= 3)
     && (not . null $ (reads (T.unpack triggerType) :: [(PostgreSqlTriggerType,String)]))
     && (all (\te ->  not . null
                   $ (reads (T.unpack te) :: [(PostgreSqlTriggerEvent,String)]))
             triggerEvns)
     && (isSqlTrigger $ T.unpack triggerFunc)

validateExtra ::  ([[Text]],Map.Map Text [[Text]])
               -> ([[Text]],Map.Map Text [[Text]])
validateExtra (attribs,extras) = let
    trigs= Map.lookup "Triggers" extras
    test = isJust (trigs) && validateTrigger trigs
         || isNothing (trigs)
    extras' = if test
                then extras
                else error $ "Invalid SQL Triggers:" ++ (show $ concat . fromJust $ trigs)
  in (attribs,extras')

persistW :: QuasiQuoter
persistW = persistWith lowerCaseSettings { validateExtras = Just validateExtra }

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
getSqlCode :: TableName -> FuncName -> [String] -> String
getSqlCode tn fn tevns = let
  tf  = maybe ""
        ( LT.unpack
        . printStatements (PrettyPrintFlags PostgreSQLDialect)
        . replicate 1
        . snd)
      $ find (\(n,_) -> n == map (toLower) fn)
      $ map (fromJust)
      $ filter (isJust)
      $ map (getSqlFuncCode) triggers
  tevns' = map (\s -> fst . head $ (reads s :: [(PostgreSqlTriggerEvent,String)])) tevns
  ct = createAfterTriggerOnRow' fn tevns' tn fn
  in tf ++ "\n" ++ ct

createAfterTriggerOnRow' name events table fn =
    LT.unpack
  . printStatements (PrettyPrintFlags PostgreSQLDialect)
  . replicate 1
  $ createAfterTriggerOnRow name events table fn

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


