module Data.Avro.Core
( putAvro, putInt, putLong, putArray, putMap )
where

import Data.Avro

import Data.Bits
import Data.Int
import Data.Word
import Data.Serialize
import Data.List (unfoldr)
import Data.Text.Lazy (Text)
import Data.Text.Lazy.Encoding (encodeUtf8)
import qualified Data.Text.Lazy as T
import qualified Data.ByteString.Lazy as B

-- | Serialize an Avro value.
--   __TO DO__: Enable policy-based chunking for arrays and map.
putAvro :: Avro -> Put
putAvro avro = case avro of
  NullV -> return ()
  BoolV value -> putWord8 $ fromIntegral $ fromEnum value
  IntV value -> putInt value
  LongV value -> putLong value
  FloatV value -> putFloat32le value
  DoubleV value -> putFloat64le value
  BytesV value -> putLong (B.length value) >> putLazyByteString value
  StringV value -> putLong (T.length value) >> putLazyByteString (encodeUtf8 value)
  UnionV index value -> putLong index >> putAvro value
  ArrayV values -> putArray 10  values   
  MapV values -> putMap 10 values
  FixedV _ value -> putLazyByteString value
  EnumV _ value -> putInt value
  RecordV _ fields -> mapM_ (putAvro . snd) fields

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

-- | Serializes an Avro array. Presently, this takes an argument for the number of items to write in each block.
--   __TODO__: Allow policy-based chunking.
putArray :: Int64 -> [Avro] -> Put
putArray n avros = do
  let (block, remainder) = splitAt (fromIntegral n) avros
  putLong $ fromIntegral $ length block
  mapM_ putAvro block
  if null remainder then putLong 0 else putArray n remainder

-- | Serializes an Avro map. Like an array, this takes an argument for the number of items to write in each block.
--   __TODO__: Allow policy-based chunking.
putMap :: Int64 -> [(Text,Avro)] -> Put
putMap n avros = do
  let (block, remainder) = splitAt (fromIntegral n) avros
  putLong $ fromIntegral $ length block
  mapM_ putKVP block
  if null remainder then putLong 0 else putMap n remainder
  where putKVP (k,v) = putAvro (StringV k) >> putAvro v
