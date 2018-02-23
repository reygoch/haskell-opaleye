{-# LANGUAGE TypeFamilies #-}

{-

TODO:

* Constant
* RunQuery
* Join

-}

module Opaleye.Internal.Map where

import           Opaleye.Column (Column, Nullable)
import qualified Opaleye.PGTypes as T

type family Map f x

type instance Map f (a, b) = (Map f a, Map f b)
