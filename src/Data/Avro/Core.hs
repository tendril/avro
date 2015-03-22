module Data.Avro.Core
( WritePolicy(..), SimpleWritePolicy(..)
, putAvro, writeAvro, putInt, putLong, defaultWritePolicy )
where

import Data.Avro

import Data.Bits
import Data.Int
import Data.Word
import Data.Serialize
import Data.List (isSuffixOf, unfoldr)
import Control.Monad.State (StateT(..),lift,gets,modify,evalStateT)
import Control.Monad (foldM,replicateM,replicateM_)
import Control.Applicative
import Data.Maybe (catMaybes)
import Data.Map (Map)
import qualified Data.Map as M
import Data.Text.Lazy (Text)
import Data.Text.Lazy.Encoding (encodeUtf8,decodeUtf8)
import Data.ByteString.Lazy (ByteString)
import qualified Data.Text.Lazy as T
import qualified Data.ByteString.Lazy as B

-- | Serialize an Avro Value. Uses @defaultWritePolicy@.
putAvro :: Avro -> Put
putAvro = writeAvro defaultWritePolicy

-- | Serializes an Avro value with a @WritePolicy@.
writeAvro :: WritePolicy a => a -> Avro -> Put
writeAvro wp = writeAvro_ wp []

-- Writes an avro with WritePolicy and path state threaded.
writeAvro_ :: WritePolicy a => a -> AvroPath -> Avro -> Put
writeAvro_ wp path avro = case avro of
  NullV -> return ()
  BoolV value -> putWord8 $ fromIntegral $ fromEnum value
  IntV value -> putInt value
  LongV value -> putLong value
  FloatV value -> putFloat32le value
  DoubleV value -> putFloat64le value
  BytesV value -> putLong (B.length value) >> putLazyByteString value
  StringV value -> putLong (T.length value) >> putLazyByteString (encodeUtf8 value)
  UnionV index value -> putLong index >> writeAvro_ wp path value
  ArrayV values -> writeArray wp path values
  MapV values -> writeMap wp path values
  FixedV _ value -> putLazyByteString value
  EnumV _ value -> putInt value
  RecordV _ fields -> mapM_ (writeAvro_ wp path . snd) fields

-- | Put an Avro @int@ value.
putInt :: Int32 -> Put
putInt value = mapM_ putWord8 $ encodeInt32 value

-- | Put an Avro @long@ value.
putLong :: Int64 -> Put
putLong value = mapM_ putWord8 $ encodeInt64 value

encodeInt32 :: Int32 -> [Word8]
encodeInt32 x = if x == 0 then [0] else encodeInt zigzagEncode32 x

encodeInt64 :: Int64 -> [Word8]
encodeInt64 x = if x == 0 then [0] else encodeInt zigzagEncode64 x

encodeInt :: (Num a, Integral a, Bits a) => (a -> a) -> a -> [Word8]
encodeInt repf = setFlags . expand . repf
  where expand = unfoldr $ \x -> if x == 0 then Nothing
          else Just (fromIntegral $ clearBit x 7, shiftR x 7)
        setFlags [] = []
        setFlags [y] = [y]
        setFlags (y:ys) = setBit y 7 : setFlags ys

-- 32-bit-wide Zig-Zag encoding
zigzagEncode32 :: Int32 -> Int32
zigzagEncode32 x = shiftL x 1 `xor` shiftR x 31

-- 64-bit-wide Zig-Zag encoding
zigzagEncode64 :: Int64 -> Int64
zigzagEncode64 x = shiftL x 1 `xor` shiftR x 63

-- | A @WritePolicy@ allows you to specify how block sizes are determined when writing Avro arrays and maps.
--   The @AvroPath@ argument allows implementations to apply policies on, for example, a per-field basis.
--   Since we don't know the full length of lists or maps ahead-of-time without evaluating the entire list,
--   and the block must be prepended with the number of items in the chunk, the interaction of the @WritePolicy@,
--   data, and block size on memory should be taken into consideration.
--   Implementations of this class must be careful to adhere to the block encoding in the Avro Specification.
class WritePolicy a where
  writeArray :: a -> AvroPath -> [Avro] -> Put
  writeMap :: a -> AvroPath -> [(Text,Avro)] -> Put

-- | @SimpleWritePolicy@ sets a simple block-size rule for either a number of items or a number of bytes per block of Avro array and map data.
--   @WritePolicyItems@ allows you to set the block size in number of items; @WritePolicyBytes@ allows you to set the block size in bytes.
--   @WritePolicyItems@ does not include the optional block byte-length information.
data SimpleWritePolicy
  = WritePolicyItems Int64
  | WritePolicyBytes Int64

instance WritePolicy SimpleWritePolicy where
  writeArray wp path avros = case wp of
    WritePolicyItems n -> putSimple True n $ map (runPutLazy . writeAvro_ wp path) avros
    WritePolicyBytes bytes -> putSimple False bytes $ map (runPutLazy . writeAvro_ wp path) avros
  writeMap wp path avros = case wp of
    WritePolicyItems n -> putSimple True n $ map (runPutLazy . putKVP) avros
    WritePolicyBytes bytes -> putSimple False bytes $ map (runPutLazy . putKVP) avros
    where putKVP (k,v) = writeAvro_ wp path (StringV k) >> writeAvro_ wp path v

-- fold over a list of serialized avros
putSimple :: Bool -> Int64 -> [ByteString] -> Put
putSimple _ _ [] = putInt 0
putSimple items size avros = do
    (nitems, nbytes, b) <- foldM f (0, 0, "") avros
    if items then putLong nitems else putLong (-nitems) >> putLong nbytes
    putLazyByteString b >> putLong 0
    where
      f (ni,nb,b) bs = if (if items then ni < size else nb < size)
        then return (ni+1, nb+B.length bs, B.append b bs)
        else putLong (-ni) >> putLong nb >> putLazyByteString b >> return (1, B.length bs, bs)

-- | A simple default write policy for convenience -- @WritePolicyItems 100@
defaultWritePolicy :: SimpleWritePolicy
defaultWritePolicy = WritePolicyItems 100

-- | A ReadPolicy determines what data, if any, are skipped, based on the Avro path. This allows for efficiently skipping large amounts of data.
--   There are two ways in which Avro data can conceivably be skipped -- one or more items in a collection (an element in an array, for example),
--   or removing something in such a way that it alters the schema. At the moment, the former is implemented. Skipping in the latter case does not
--   alter the schema, and will only skip values that can be quite large -- bytes, entire arrays, etc -- and will simply return empty but valid values.
class ReadPolicy a where
  readskip :: a -> AvroPath -> Bool

-- As forward references are not supported by Avro, it is necessary to maintain a stateful list of bindings while traversing the schema.
data GetState = GetState
  { gsBindings :: Map Text Schema
  } deriving Show

-- | Deserialize an Avro value for a given schema. Uses @defaultReadPolicy.
getAvro :: Schema -> Get Avro
getAvro schema = evalStateT (readAvro_ defaultReadPolicy [] schema) (GetState M.empty)

-- | Deserialize Avro data for a given schema, with a specified ReadPolicy.

readAvro :: ReadPolicy a => a -> Schema -> Get Avro
readAvro rp schema = evalStateT (readAvro_ rp [] schema) (GetState M.empty)

-- The internal workhorse for reading Avro data.
readAvro_ :: ReadPolicy a => a -> AvroPath -> Schema -> StateT GetState Get Avro
readAvro_ rp path schema = case schema of
  NULL -> return NullV
  BOOL -> lift $ BoolV . (== 1) <$> getWord8
  INT -> lift $ IntV <$> getInt
  LONG -> lift $ LongV <$> getLong
  FLOAT -> lift $ FloatV <$> getFloat32le
  DOUBLE -> lift $ DoubleV <$> getFloat64le
  -- __TO DO__: maxBound?
  BYTES -> lift $ if readskip rp path
             then getLong >>= skip . fromIntegral >> return (BytesV "")
             else BytesV <$> (getLong >>= getLazyByteString)
  STRING -> lift $ StringV . decodeUtf8 <$> (getLong >>= getLazyByteString)
  UNION ss -> do
    idx <- lift $ fromIntegral <$> getLong
    if fromIntegral idx < length ss
      then UnionV idx <$> readAvro_ rp (AvroUnion idx:path) (ss !! fromIntegral idx)
      else lift $ fail "Union value has index value greater than its number of schemas."
  ARRAY s -> ArrayV <$> readArray rp path 0 s
  MAP s -> MapV <$> foldr (\a b -> if readskip rp (AvroMapKey (fst a):path) then b else a:b) [] <$> readMap rp path s
  FIXED n size _ _ -> bind n >> FixedV n <$> lift (getLazyByteString size)
  ENUM n v ns _ _ -> bind n >> EnumV n <$> lift getInt
  RECORD n fields ns _ _ -> bind n >> RecordV n <$> zip (map fieldName fields) <$>   mapM (uncurry (readAvro_ rp) . (\ a -> (AvroField (fieldName a) : path, fieldType a))) fields
  REFERENCE n -> do
    names <- gets gsBindings
    case M.lookup n names of
      Nothing -> lift (fail $ "type " ++ show n ++ " does not exist")
      Just s -> readAvro_ rp path s
  where
    -- Bind a schema to a name.
    bind :: Text -> StateT GetState Get ()
    bind n = do
      bs <- gets gsBindings
      modify $ \x -> x { gsBindings = M.insert n schema bs }

readArray :: ReadPolicy a => a -> AvroPath -> Int64 -> Schema -> StateT GetState Get [Avro]
readArray rp path idx schema = do
  len <- lift $ fromIntegral <$> getLong
  case compare len 0 of
    EQ -> return []
    GT -> if readskip rp path
            then replicateM_ len (readAvro_ rp path schema) >> readArray rp path (idx+fromIntegral len) schema
            else (++) <$> catMaybes <$> sequence (foldr ( \(b,c) a->
              if readskip rp (b:path)
                then (c (b:path) schema >> return Nothing):a
                else (Just <$> c (b:path) schema):a)
              [] (zip (map AvroArrayIndex [idx..]) (replicate len (readAvro_ rp) ))) <*> readArray rp path (idx+fromIntegral len) schema
    LT -> if readskip rp path
            then lift ( getLong >>= skip . fromIntegral . abs) >> readArray rp path idx schema
            else lift getLong >> (++) <$> catMaybes <$> sequence (foldr ( \(b,c) a->
              if readskip rp (b:path)
                then (c (b:path) schema >> return Nothing):a
                else (Just <$> c (b:path) schema):a)
              [] (zip (map AvroArrayIndex [idx..]) (replicate len (readAvro_ rp) ))) <*> readArray rp path (idx+fromIntegral len) schema

readMap :: ReadPolicy a => a -> AvroPath -> Schema -> StateT GetState Get [(Text, Avro)]
readMap rp path schema = do
  len  <- lift $ fromIntegral <$> getLong
  case compare len 0 of
    EQ -> return []
    GT -> if readskip rp path
      then replicateM_ len getKVP >> readMap rp path schema
      else (++) <$> catMaybes <$> replicateM len getKVP <*> readMap rp path schema
    LT -> if readskip rp path
      then lift (getLong >>= skip . fromIntegral . abs) >> readMap rp path schema
      else lift getLong >> (++) <$> catMaybes <$> replicateM len getKVP <*> readMap rp path schema
  where getKVP = do
          k<-stringv <$> readAvro_ rp path STRING
          if readskip rp ((AvroMapKey k):path)
            then readAvro_ rp ((AvroMapKey k):path) schema >> return Nothing
            else Just <$> (,) k <$> readAvro_ rp ((AvroMapKey k):path) schema

-- | ReadAll skips no data
data ReadAll = ReadAll

instance ReadPolicy ReadAll where
  readskip _ = const False

defaultReadPolicy :: ReadAll
defaultReadPolicy = ReadAll

-- | A SkipList skips the specified Avro paths..
data SkipList = SkipList [AvroPath]

instance ReadPolicy SkipList where
  readskip (SkipList paths) path = True `elem` map (`isSuffixOf` path) paths

getInt :: Get Int32
getInt = zigzagDecode . merge <$> getVLE

getLong :: Get Int64
getLong = zigzagDecode . merge <$> getVLE

zigzagDecode :: (Bits a, Num a) => a -> a
zigzagDecode n = shiftR n 1 `xor` (-(n .&. 1))

merge :: (Num a, Bits a) => [Word8] -> a
merge = foldr1 (.|.) . map (\(n,b) -> shift b (n*7)) . zip [0..]
      . map (fromIntegral . flip clearBit 7)

getVLE :: Get [Word8]
getVLE = do
  byte <- getWord8
  if testBit byte 7
    then (byte:) <$> getVLE
    else return [byte]
