{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Triggers where

import Data.Maybe

import Database.HsSqlPpp.Quote
import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Annotation

data PostgreSqlTriggerType = BEFORE | AFTER | INSTEADOF
  deriving (Eq,Read)

instance Show PostgreSqlTriggerType where
  show BEFORE    = "BEFORE"
  show AFTER     = "AFTER"
  show INSTEADOF = "INSTEAD OF"

data PostgreSqlTriggerEvent =  INSERT | UPDATE | DELETE | TRUNCATE | OR
  deriving (Eq,Show,Read)

tableIdTrig :: Statement
tableIdTrig = [sqlStmt|
    CREATE OR REPLACE FUNCTION tableIdTrig()
      RETURNS trigger AS
    $BODY$
      BEGIN
        PERFORM pg_notify(TG_TABLE_NAME,NEW.id::TEXT);
        RETURN NULL;
      END;
    $BODY$
      LANGUAGE plpgsql VOLATILE;
    |]

tableTrig :: Statement
tableTrig = [sqlStmt|
    CREATE OR REPLACE FUNCTION tableTrig()
      RETURNS trigger AS
    $BODY$
      BEGIN
        PERFORM pg_notify(TG_TABLE_NAME,TG_TABLE_NAME);
        RETURN NULL;
      END;
    $BODY$
      LANGUAGE plpgsql VOLATILE;
    |]

notTrig :: Statement
notTrig = [sqlStmt|
    CREATE OR REPLACE FUNCTION notTrig()
      RETURNS INT AS
    $BODY$
      BEGIN
        PERFORM pg_notify(TG_TABLE_NAME,TG_TABLE_NAME);
        RETURN NULL;
      END;
    $BODY$
      LANGUAGE plpgsql VOLATILE;
    |]


triggers :: [Statement]
triggers = [tableTrig, tableIdTrig, notTrig]

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
  name  = Nmc name'
  events= convertTriggerEvents events'
  table = Name emptyAnnotation [Nmc table']
  fn    = Name emptyAnnotation [Nmc fn']
  in CreateTrigger
      emptyAnnotation
      name
      TriggerAfter
      events
      table
      EachRow
      fn []

  {-in [sqlStmt|-}
        {-CREATE TRIGGER $m(name) AFTER $t(events)-}
          {-ON $n(table)-}
          {-FOR EACH ROW-}
          {-EXECUTE PROCEDURE $n(fn)();-}
  {-|]-}

