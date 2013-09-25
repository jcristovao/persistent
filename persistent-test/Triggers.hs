{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Triggers where


import Database.HsSqlPpp.Quote
import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Parser
import Text.Shakespeare.Text
import Data.Either
import Data.Text.Lazy (Text)

tableIdTrigT :: Text
tableIdTrigT = [lt|
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

tableIdTrig :: Statement
tableIdTrig = let
  res = parsePlpgsql
    (ParseFlags PostgreSQLDialect)
    __FILE__
    Nothing
    tableIdTrigT
  in either (\pe -> error $ show pe) head res


{-tableIdTrig :: Statement-}
{-tableIdTrig = [sqlStmt|-}
    {-CREATE OR REPLACE FUNCTION tableIdTrig()-}
      {-RETURNS trigger AS-}
    {-$BODY$-}
      {-BEGIN-}
        {-PERFORM pg_notify(TG_TABLE_NAME,NEW.id::TEXT);-}
        {-RETURN NULL;-}
      {-END;-}
    {-$BODY$-}
      {-LANGUAGE plpgsql VOLATILE;-}
    {-|]-}

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


