module Data.Avro.Core
( WritePolicy(..), SimpleWritePolicy(..)
, putAvro, writeAvro, putInt, putLong, defaultWritePolicy )
where

import Data.Avro

import Data.Bits
import Data.Int
import Data.Word
import Data.Serialize
import Control.Monad (foldM)
import Data.List (unfoldr)
import Data.Text.Lazy (Text)
import Data.Text.Lazy.Encoding (encodeUtf8)
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
--   __TO DO__: Monadize this to thread path state and policy
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
  UnionV n index value -> putLong index >> writeAvro_ wp (AvroUnion n:path) value
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
defaultWritePolicy = WritePolicyItems 100
