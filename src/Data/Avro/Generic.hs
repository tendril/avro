{-# LANGUAGE
    DeriveGeneric
  , FlexibleInstances
  , ScopedTypeVariables,
  , TypeOperators
#-}

module Data.Avro.Generic where

import Data.Avro

import Data.Int
import Data.ByteString.Lazy (ByteString)
import Data.Text.Lazy (pack, unpack)
import Data.Map (Map)
import Generics.Deriving.Base

data MD5 = MD5 ByteString
  deriving (Generic,Show)

data HR = HR
  { clientHash :: MD5
  , clientProtocol :: Maybe String
  , serverHash :: Maybe MD5
  , meta :: Maybe (Map String ByteString)
  } deriving (Show,Generic)

class Avroo a where
  schematize :: a -> Schema
  serialize :: a -> Avro
  deserialize :: Avro -> a
instance Avroo () where
  schematize _ = NULL
  serialize () = NullV
  deserialize NullV = ()
instance Avroo Bool where
  schematize _ = BOOL
  serialize = BoolV
  deserialize (BoolV a) = a
instance Avroo Int where
  schematize _ = INT
  serialize = IntV . fromIntegral
  deserialize (IntV a) = fromIntegral a
instance Avroo Int32 where
  schematize _ = INT
  serialize = IntV
  deserialize (IntV a) = a
instance Avroo Int64 where
  schematize _ = LONG
  serialize = LongV
  deserialize (LongV a) = a
instance Avroo Float where
  schematize _ = FLOAT
  serialize = FloatV
  deserialize (FloatV a) = a
instance Avroo Double where
  schematize _ = DOUBLE
  serialize = DoubleV
  deserialize (DoubleV a) = a
instance Avroo ByteString where
  schematize _ = BYTES
  serialize = BytesV
  deserialize (BytesV a) = a
instance Avroo String where
  schematize _ = STRING
  serialize = StringV . pack
  deserialize (StringV a) = unpack a
instance Avroo a => Avroo [a] where
  schematize (_::[a]) = ARRAY $ schematize (undefined :: a)
  serialize as = ArrayV $ fmap serialize as
  deserialize (ArrayV as) = fmap deserialize as
instance Avroo a => Avroo [(String,a)] where
  schematize (_::[(String,a)]) = MAP $ schematize (undefined :: a)
  serialize as = MapV $ fmap (\(k,v) -> (pack k, serialize v)) as
  deserialize (MapV as) = fmap (\(k,v) -> (unpack k, deserialize v)) as
instance Avroo a => Avroo (Maybe a) where
  schematize (_:: Maybe a) = UNION [NULL, schematize (undefined :: a)]
  serialize Nothing = UnionV 0 NullV 
  serialize (Just a) = UnionV 1 $ serialize a
  deserialize (UnionV 0 NullV) = Nothing
  deserialize (UnionV 1 a) = Just $ deserialize a

class GAvro f where
  gSchematize :: f a -> Schema

instance (GFields f, Datatype c) => GAvro (D1 c f) where
  gSchematize (dtn :: (D1 c f) a) = RECORD (pack $ datatypeName dtn) (gFields (undefined :: f a)) (Just $ pack $ moduleName dtn) Nothing Nothing

instance (Constructor c) =>  GAvro (C1 c U1) where
  gSchematize (x :: (C1 c U1) a) = RECORD (pack $ conName x) [] Nothing Nothing Nothing

class GFields f where
  gFields :: f a -> [Field]

instance (GFields f) => GFields (C1 c f) where
  gFields (x :: (C1 c f) a) = (gFields (undefined :: f a))

instance (GAvro f, Selector s) => GFields (S1 s f) where
  gFields (x :: (S1 s f) a ) = [Field (pack $ selName x) (gSchematize (undefined :: f a)) Nothing Nothing Nothing Nothing]

instance (GFields f, GFields g) => GFields (f :*: g) where
  gFields (_ :: (f :*: g) a) = gFields (undefined :: f a) ++ gFields (undefined :: g a)

instance (GAvro f, GAvro g) => GAvro (f :+: g) where
  gSchematize _ = unite (gSchematize (undefined :: f a)) (gSchematize (undefined :: g a))
    where
      unite a (UNION bs) = UNION $ a : bs
      unite a b = UNION [a,b]

instance (GAvro f, Constructor c, Selector s) => GAvro (C1 c (S1 s f)) where
   gSchematize (x :: (C1 c (S1 s f) a)) = gSchematize $ unM1 x

instance (GAvro f, Selector s) => GAvro (S1 s f) where
  gSchematize (x :: (S1 s f) a) = FIXED (pack $ selName x) 1 Nothing Nothing

instance (Avroo f) =>  GAvro (K1 r f) where
   gSchematize (x :: (K1 r f) a) = schematize (undefined :: f)
