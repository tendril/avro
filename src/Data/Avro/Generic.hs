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
import Data.Text.Lazy (Text, pack, unpack,    append)
import Data.Map (Map)
import GHC.Generics
import Data.Either (rights,either)
import Control.Monad.State.Lazy
import qualified Data.Map as Map

class AvroType a where
  avroType :: a -> Schema
  
  default avroType :: (Generic a, GAvroType (Rep a)) => a -> Schema
  avroType = gRecord . from -- REFERENCE . gName . from

instance AvroType () where
  avroType _ = NULL
instance AvroType Bool where
  avroType _ = BOOL
instance AvroType Int where
  avroType _ = INT
instance AvroType Int32 where
  avroType _ = INT
instance AvroType Int64 where
  avroType _ = LONG
instance AvroType Float where
  avroType _ = FLOAT
instance AvroType Double where
  avroType _ = DOUBLE
instance AvroType ByteString where
  avroType _ = BYTES
instance AvroType String where
  avroType _ = STRING
instance AvroType a => AvroType [a] where
  avroType (_::[a]) = ARRAY $ avroType (undefined :: a)
instance AvroType a => AvroType [(String,a)] where
  avroType (_::[(String,a)]) = MAP $ avroType (undefined :: a)
instance AvroType a => AvroType (Maybe a) where
  avroType (_:: Maybe a) = UNION [NULL, avroType (undefined :: a)]

class AvroType a => AvroRecordType a where
  avroRecordType :: a -> Schema
  avroRecordType2 :: a -> Schema

  default avroRecordType :: (Generic a, GAvroType (Rep a)) => a -> Schema
  avroRecordType a = gRecord $ from a
  default avroRecordType2 :: (Generic a, GAvroType (Rep a)) => a -> Schema
  avroRecordType2 a = evalState (gRecord2 $ from a) (Map.fromList [(gName $ from a,Nothing)])

class GAvroType f where
  gName :: f a -> Text
  gAvroType :: f a -> Schema
  gRecord :: f a -> Schema
  gRecord2 :: f a -> State (Map.Map Text (Maybe Schema)) Schema

instance (Datatype c, GAvroType f,GFields f) => GAvroType (D1 c f) where
  gAvroType a = gAvroType $ unM1 a
  gName a = gName $ unM1 a
  gRecord (a :: (D1 c f) a) = gRecord $ unM1 a
  gRecord2 (a :: (D1 c f) a) = do
    m <- get
    case Map.lookup (gName a) m of
      -- case where it is the first time encountering this type
      Nothing -> do
        let (r,s) = runState (gRecord2 $ unM1 a) m
        put $ Map.union m s
        return r
      Just b -> case b of
        -- root schema
        Nothing -> gRecord2 $ unM1 a
        -- already exists? use a reference
        Just c -> return $ REFERENCE $ gName a

--instance (GFields f, GFields g) => GAvroType (f :+: g) where

instance (GFields f, Constructor c) => GAvroType (C1 c f) where
  gName = pack . conName
  gAvroType = REFERENCE . pack . conName
  gRecord (a :: (C1 c f) a) = RECORD (pack $ conName a) (gFields (undefined :: f a)) Nothing Nothing Nothing
  gRecord2 a = do
    fs <- gFields2 $ unM1 a
    return $ RECORD (gName a) fs Nothing Nothing Nothing

class GFields f where
  gFields :: f a -> [Field]
  gFields2 :: f a -> State (Map.Map Text (Maybe Schema)) [Field]

instance (GFields f, Constructor c) => GFields (C1 c f) where
  gFields a = gFields $ unM1 a
  gFields2 a = gFields2 $ unM1 a

instance (GFields f, GFields g) => GFields (f :*: g) where
  gFields (_ :: (f :*: g) a) = gFields (undefined :: f a) ++ gFields (undefined :: g a)
  gFields2 (_ :: (f :*: g) a) = do
    l<-gFields2 (undefined :: f a)
    r<- gFields2 (undefined :: g a)
    return $ l ++ r

instance (GType f, Selector s) => GFields (S1 s f) where
  gFields a = [gField a]
  gFields2 a = do
    f<-gField2 a
    return [f]

class GField f where
  gField :: f a -> Field
  gField2 :: f a -> State (Map.Map Text (Maybe Schema)) Field

instance (GType f, Selector s) => GField (S1 s f) where
  gField a = Field (pack $ selName a) (gFieldType (undefined::f a)) Nothing Nothing Nothing Nothing
  gField2 a = do
    f <- gFieldType2 (undefined :: f a)
    return $ Field (pack $ selName a) f Nothing Nothing Nothing Nothing

class GType f where
  gFieldType :: f a -> Schema
  gFieldType2 :: f a -> State (Map.Map Text (Maybe Schema)) Schema

instance (Generic f, AvroType f, GAvroType (Rep f)) =>  GType (K1 r f) where
  gFieldType a = avroType (undefined::f)
  gFieldType2 a = do
    let at = avroType $ unK1 a
    case at of
      REFERENCE _ -> do
        m<- get
        case Map.lookup (gName $ from $ unK1 a) m of
          Nothing -> do
            put $ Map.insert (gName $ from $ unK1 a) Nothing m
            let  (r,s) = runState (gRecord2 $ from $ unK1 a) m -- get full schema
            put $ Map.insert (gName $ from $ unK1 a) (Just r) m
            return r
          Just b -> return $ REFERENCE $ gName $ from $ unK1 a
      _ -> return at

-- implement StateT update
instance (GUnion f, GUnion g) => GType (f :+: g) where
  gFieldType2 (_:: (f :+: g) a) = return $ UNION $ gUnion ( undefined :: f a) ++ gUnion (undefined :: g a)

class GUnion f where
  gUnion :: f a -> [Schema]

instance (Datatype c, GUnion f) => GUnion (D1 c f) where
  gUnion a = gUnion (undefined :: f a)

instance (Constructor c) => GUnion (C1 c f) where
  gUnion a = [REFERENCE $ pack $ conName a]

instance (GUnion f, GUnion g) => GUnion (f :+: g) where
  gUnion (_ :: (f :+: g) a) = gUnion (undefined :: f a) ++ gUnion (undefined :: g a)

