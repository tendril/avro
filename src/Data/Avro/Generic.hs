{-# LANGUAGE
    DeriveGeneric
  , FlexibleInstances
  , ScopedTypeVariables
  , TypeOperators
  , DefaultSignatures
  , FlexibleContexts
  , IncoherentInstances
  #-}

module Data.Avro.Generic where

import Data.Avro

import Data.Int
import Data.ByteString.Lazy (ByteString)
import Data.Text.Lazy (Text, pack, unpack)
import Data.Map (Map)
import GHC.Generics
import Data.Either (rights,either)

class Avroo a where
  schematize :: a -> Schema
  serialize :: a -> Avro
  deserialize :: Avro -> Either String a

  default schematize :: (Generic a, GAvro (Rep a)) => a -> Schema
  schematize a = gSchematize $ from a
  default serialize :: (Generic a, GAvro (Rep a)) => a -> Avro
  serialize a = gSerialize $ from a
  default deserialize :: (Generic a, GAvro (Rep a)) => Avro -> Either String a
  deserialize a = Right $ to $ gDeserialize a

instance Avroo () where
  schematize _ = NULL
  serialize () = NullV
  deserialize NullV = Right ()
  deserialize a = Left $ "Cannot deserialize " ++ show a ++ " to NULL"

instance Avroo Bool where
  schematize _ = BOOL
  serialize = BoolV
  deserialize (BoolV a) = Right a
instance Avroo Int where
  schematize _ = INT
  serialize = IntV . fromIntegral
  deserialize (IntV a) = Right $ fromIntegral a
instance Avroo Int32 where
  schematize _ = INT
  serialize = IntV
  deserialize (IntV a) = Right a
instance Avroo Int64 where
  schematize _ = LONG
  serialize = LongV
  deserialize (LongV a) = Right a
instance Avroo Float where
  schematize _ = FLOAT
  serialize = FloatV
  deserialize (FloatV a) = Right a
instance Avroo Double where
  schematize _ = DOUBLE
  serialize = DoubleV
  deserialize (DoubleV a) = Right a
instance Avroo ByteString where
  schematize _ = BYTES
  serialize = BytesV
  deserialize (BytesV a) = Right a
instance Avroo String where
  schematize _ = STRING
  serialize = StringV . pack
  deserialize (StringV a) = Right $ unpack a
-- TODO: more robust way of handling malformed ArrayV values
instance Avroo a => Avroo [a] where
  schematize (_::[a]) = ARRAY $ schematize (undefined :: a)
  serialize as = ArrayV $ fmap serialize as
  deserialize (ArrayV as) = Right $ rights $ fmap deserialize as
-- TODO: more robust way of handling malformed MapV values
instance Avroo a => Avroo [(String,a)] where
  schematize (_::[(String,a)]) = MAP $ schematize (undefined :: a)
  serialize as = MapV $ fmap (\(k,v) -> (pack k, serialize v)) as
  deserialize (MapV as) = Right $ foldl (\a (k,v) -> either (const a) (\t->(k,t):a) v) [] $ fmap (\(k,v) -> (unpack k, deserialize v)) as
instance Avroo a => Avroo (Maybe a) where
  schematize (_:: Maybe a) = UNION [NULL, schematize (undefined :: a)]
  serialize Nothing = UnionV 0 NullV 
  serialize (Just a) = UnionV 1 $ serialize a
  deserialize (UnionV 0 NullV) = Right Nothing
  deserialize (UnionV 1 a) = either (\a -> Left a) (\a -> Right $ Just a) $ deserialize a

class GAvro f where
  gSchematize :: f a -> Schema
  gSerialize :: f a -> Avro
  gDeserialize :: Avro -> f a

instance (GFields f, Datatype c) => GAvro (D1 c f) where
  gSchematize (dtn :: (D1 c f) a) = RECORD (pack $ datatypeName dtn) (gFields (undefined :: f a)) (Just $ pack $ moduleName dtn) Nothing Nothing
  gSerialize (x:: (D1 c f) a) = RecordV (pack $ datatypeName x ) $ gFs $ (unM1 x :: f a)
  -- add check that conname matches
  gDeserialize (RecordV _ fs) = M1 $ gDs fs

instance (Constructor c) =>  GAvro (C1 c U1) where
  gSchematize (x :: (C1 c U1) a) = RECORD (pack $ conName x) [] Nothing Nothing Nothing

class GFields f where
  gFields :: f a -> [Field]
  gFs :: f a -> [(Text,Avro)]
  gDs :: [(Text,Avro)] -> f a

instance (GFields f) => GFields (C1 c f) where
  gFields (x :: (C1 c f) a) = (gFields (undefined :: f a))
  gFs x = gFs $ unM1 x
  gDs fs = M1 $ gDs fs

instance (GAvro f, Selector s) => GFields (S1 s f) where
  gFields (x :: (S1 s f) a ) = [Field (pack $ selName x) (gSchematize (undefined :: f a)) Nothing Nothing Nothing Nothing]
  gFs x = [(pack $ selName x, gSerialize $ unM1 x )]
  -- need to check selector names
  gDs fs  = M1 $ gDeserialize $ snd $ head fs

instance (GFields f, GFields g) => GFields (f :*: g) where
  gFields (_ :: (f :*: g) a) = gFields (undefined :: f a) ++ gFields (undefined :: g a)
  gFs (f :*: g) = gFs f ++ gFs g
  gDs (a:bs) = gDs [a] :*: gDs bs

instance (GAvro f, GAvro g) => GAvro (f :+: g) where
  gSchematize _ = unite (gSchematize (undefined :: f a)) (gSchematize (undefined :: g a))
    where
      unite a (UNION bs) = UNION $ a : bs
      unite a b = UNION [a,b]

instance (GAvro f, Constructor c, Selector s) => GAvro (C1 c (S1 s f)) where
   gSchematize (x :: (C1 c (S1 s f) a)) = gSchematize $ unM1 x

instance (GAvro f, Selector s) => GAvro (S1 s f) where
  gSchematize (x :: (S1 s f) a) = FIXED (pack $ selName x) 1 Nothing Nothing

instance (Generic f, Avroo f) =>  GAvro (K1 r f) where
  gSchematize (x :: (K1 r f) a) = schematize (undefined :: f)
  gSerialize x = serialize (unK1 x :: f)
  gDeserialize x = case deserialize x of
    -- TODO: propagate errors upward
    Left e -> K1 undefined
    Right v -> K1 v
