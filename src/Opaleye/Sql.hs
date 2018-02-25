{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}

module Opaleye.Sql where

import qualified Opaleye.Internal.Unpackspec as U
import           Opaleye.Internal.ShowSql (formatAndShowSQL)
import qualified Opaleye.Internal.Optimize as Op
import           Opaleye.Internal.Helpers ((.:))
import qualified Opaleye.Internal.QueryArr as Q

import qualified Data.Profunctor.Product.Default as D

-- * Showing SQL

-- | Show the SQL query string generated from the query.
--
-- When 'Nothing' is returned it means that the 'Query' returns zero
-- rows.
--
-- Example type specialization:
--
-- @
-- showSql :: Query (Column a, Column b) -> Maybe String
-- @
--
-- Assuming the @makeAdaptorAndInstance@ splice has been run for the
-- product type @Foo@:
--
-- @
-- showSql :: Query (Foo (Column a) (Column b) (Column c)) -> Maybe String
-- @
showSql :: forall columns.
           D.Default U.Unpackspec columns columns
        => Q.Query columns
        -> Maybe String
showSql = showSqlExplicit (D.def :: U.Unpackspec columns columns)

-- | Show the unoptimized SQL query string generated from the query.
showSqlUnopt :: forall columns.
                D.Default U.Unpackspec columns columns
             => Q.Query columns
             -> Maybe String
showSqlUnopt = showSqlUnoptExplicit (D.def :: U.Unpackspec columns columns)

-- * Explicit versions

showSqlExplicit :: U.Unpackspec columns b -> Q.Query columns -> Maybe String
showSqlExplicit = formatAndShowSQL
                  . (\(x, y, z) -> (x, Op.optimize y, z))
                  .: Q.runQueryArrUnpack

showSqlUnoptExplicit :: U.Unpackspec columns b -> Q.Query columns -> Maybe String
showSqlUnoptExplicit = formatAndShowSQL .: Q.runQueryArrUnpack
