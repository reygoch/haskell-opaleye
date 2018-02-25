module Opaleye.Internal.ShowSql where

import qualified Opaleye.Internal.HaskellDB.PrimQuery as HPQ
import qualified Opaleye.Internal.Optimize            as Op
import qualified Opaleye.Internal.PrimQuery           as PQ
import qualified Opaleye.Internal.Print               as Pr
import qualified Opaleye.Internal.Sql                 as Sql
import qualified Opaleye.Internal.Tag                 as T

-- Deprecated
formatAndShowSQL :: ([HPQ.PrimExpr], PQ.PrimQuery' a, T.Tag) -> Maybe String
formatAndShowSQL = fmap (show . Pr.ppSql . Sql.sql) . traverse2Of3 Op.removeEmpty
  where -- Just a lens
        traverse2Of3 :: Functor f => (a -> f b) -> (x, a, y) -> f (x, b, y)
        traverse2Of3 f (x, y, z) = fmap (\y' -> (x, y', z)) (f y)

