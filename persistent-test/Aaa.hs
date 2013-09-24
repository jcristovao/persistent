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

import Data.Maybe
import Data.List
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import Data.Text (Text)
import Data.Conduit

import Database.HsSqlPpp.Quote
import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Pretty
import Data.Char

import Triggers (triggers)

data PostgreSqlTriggerType = BEFORE | AFTER | INSTEADOF
  deriving (Eq,Show,Read)

validateTrigger :: Maybe [[Text]]
                -> Bool
validateTrigger Nothing = False
validateTrigger (Just ps) = let
  params = concat ps
  triggerType = params !! 0
  triggerFunc = params !! 1
  in    (length params == 2)
     && (not . null $ (reads (T.unpack triggerType) :: [(PostgreSqlTriggerType,String)]))
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

{-getSqlFuncName :: Statement -> Maybe String-}
{-getSqlFuncName sql = case sql of-}
   {-(CreateFunction _ (Name _ ns) _ _ _ _ _ _)-}
      {--> if null ns then Nothing else Just (ncStr $ last ns)-}
   {-_  -> Nothing-}

{-getSqlFuncType :: Statement -> Maybe String-}
{-getSqlFuncType sql = case sql of-}
  {-(CreateFunction _ _ _ (SimpleTypeName _ (Name _ ns)) _ _ _ _)-}
    {--> if null ns-}
          {-then Nothing-}
          {-else Just (ncStr $ last ns)-}
  {-_ -> Nothing-}

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


isSqlTrigger :: String -> Bool
isSqlTrigger name = any (== Just (map toLower name, map toLower "trigger"))
                  $ fmap (getSqlFuncAttrs) triggers

getSqlCode :: String -> String
getSqlCode name = maybe "" (LT.unpack . printStatements (PrettyPrintFlags PostgreSQLDialect) . replicate 1 . snd)
                $ find (\(n,_) -> n == map (toLower) name)
                $ map (fromJust)
                $ filter (isJust)
                $ map (getSqlFuncCode) triggers

{-getSqlTriggers :: [String]-}
{-getSqlTriggers = filter (not . null)-}
               {-$ fmap (maybe "" id . getSqlFuncName) triggers-}

------------------------------------------------------------------------------
-- PostgreSQL replacements ---------------------------------------------------
------------------------------------------------------------------------------


