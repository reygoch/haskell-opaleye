{-# LANGUAGE FlexibleContexts, FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module Opaleye.Internal.Join where

import qualified Opaleye.Internal.HaskellDB.PrimQuery as HPQ
import qualified Opaleye.Internal.Map                 as Map
import qualified Opaleye.Internal.PackMap             as PM
import qualified Opaleye.Internal.Tag                 as T
import qualified Opaleye.Internal.Unpackspec          as U
import           Opaleye.Internal.Column (Column(Column), Nullable)
import qualified Opaleye.Internal.QueryArr as Q
import qualified Opaleye.Internal.PrimQuery as PQ
import qualified Opaleye.PGTypes as T
import qualified Opaleye.Column as C

import qualified Control.Applicative as A

import           Data.Profunctor (Profunctor, dimap)
import qualified Data.Profunctor.Product as PP
import qualified Data.Profunctor.Product.Default as D

newtype NullMaker a b = NullMaker (a -> b)

toNullable :: NullMaker a b -> a -> b
toNullable (NullMaker f) = f

instance D.Default NullMaker (Column a) (Column (Nullable a)) where
  def = NullMaker C.toNullable

instance D.Default NullMaker (Column (Nullable a)) (Column (Nullable a)) where
  def = NullMaker id

joinExplicit :: U.Unpackspec columnsA columnsA
             -> U.Unpackspec columnsB columnsB
             -> (columnsA -> returnedColumnsA)
             -> (columnsB -> returnedColumnsB)
             -> PQ.JoinType
             -> Q.Query columnsA -> Q.Query columnsB
             -> ((columnsA, columnsB) -> Column T.PGBool)
             -> Q.Query (returnedColumnsA, returnedColumnsB)
joinExplicit uA uB returnColumnsA returnColumnsB joinType
             qA qB cond = Q.simpleQueryArr q where
  q ((), startTag) = ((nullableColumnsA, nullableColumnsB), primQueryR, T.next endTag)
    where (columnsA, primQueryA, midTag) = Q.runSimpleQueryArr qA ((), startTag)
          (columnsB, primQueryB, endTag) = Q.runSimpleQueryArr qB ((), midTag)

          (newColumnsA, ljPEsA) =
            PM.run (U.runUnpackspec uA (extractLeftJoinFields 1 endTag) columnsA)
          (newColumnsB, ljPEsB) =
            PM.run (U.runUnpackspec uB (extractLeftJoinFields 2 endTag) columnsB)

          nullableColumnsA = returnColumnsA newColumnsA
          nullableColumnsB = returnColumnsB newColumnsB

          Column cond' = cond (columnsA, columnsB)
          primQueryR = PQ.Join joinType cond' ljPEsA ljPEsB primQueryA primQueryB

extractLeftJoinFields :: Int
                      -> T.Tag
                      -> HPQ.PrimExpr
                      -> PM.PM [(HPQ.Symbol, HPQ.PrimExpr)] HPQ.PrimExpr
extractLeftJoinFields n = PM.extractAttr ("result" ++ show n ++ "_")

-- { Boilerplate instances

instance Functor (NullMaker a) where
  fmap f (NullMaker g) = NullMaker (fmap f g)

instance A.Applicative (NullMaker a) where
  pure = NullMaker . A.pure
  NullMaker f <*> NullMaker x = NullMaker (f A.<*> x)

instance Profunctor NullMaker where
  dimap f g (NullMaker h) = NullMaker (dimap f g h)

instance PP.ProductProfunctor NullMaker where
  empty  = PP.defaultEmpty
  (***!) = PP.defaultProfunctorProduct

--

data Nulled

-- It's quite unfortunate that we have to write these out by hand
-- until we probably do nullability as a distinction between
--
-- Column (Nullable a)
-- Column (NonNullable a)

type instance Map.Map Nulled (Column (Nullable a)) = Column (Nullable a)

type instance Map.Map Nulled (Column T.PGInt4) = Column (Nullable T.PGInt4)
type instance Map.Map Nulled (Column T.PGInt8) = Column (Nullable T.PGInt8)
type instance Map.Map Nulled (Column T.PGText) = Column (Nullable T.PGText)
type instance Map.Map Nulled (Column T.PGFloat8) = Column (Nullable T.PGFloat8)
type instance Map.Map Nulled (Column T.PGBool) = Column (Nullable T.PGBool)
type instance Map.Map Nulled (Column T.PGUuid) = Column (Nullable T.PGUuid)
type instance Map.Map Nulled (Column T.PGBytea) = Column (Nullable T.PGBytea)
type instance Map.Map Nulled (Column T.PGText) = Column (Nullable T.PGText)
type instance Map.Map Nulled (Column T.PGDate) = Column (Nullable T.PGDate)
type instance Map.Map Nulled (Column T.PGTimestamp) = Column (Nullable T.PGTimestamp)
type instance Map.Map Nulled (Column T.PGTimestamptz) = Column (Nullable T.PGTimestamptz)
type instance Map.Map Nulled (Column T.PGTime) = Column (Nullable T.PGTime)
type instance Map.Map Nulled (Column T.PGCitext) = Column (Nullable T.PGCitext)
type instance Map.Map Nulled (Column T.PGJson) = Column (Nullable T.PGJson)
type instance Map.Map Nulled (Column T.PGJsonb) = Column (Nullable T.PGJsonb)
