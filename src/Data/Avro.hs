module Data.Avro
( Schema(..), Field(..), Order(..), Avro(..), BlockLength(..), AvroPath, AvroPathElement(..), Container(..) , Compression(..))
where

import Data.Text.Lazy  (Text(..),fromChunks,toChunks)
import Data.Int
import Data.ByteString.Lazy (ByteString, toChunks)
import Data.Aeson
import Control.Applicative ((<$>),(<*>),empty,pure)
--import Data.Text (Text(..),unpack)
import Data.Vector (fromList,toList)
import Data.Maybe

import Data.Scientific (toRealFloat,fromFloatDigits)

import Data.Text.Encoding

-- | Avro Schema
data Schema
  = NULL | BOOL | INT | LONG | FLOAT | DOUBLE | BYTES | STRING
  | UNION [Schema] | ARRAY Schema | MAP Schema
  | FIXED
  { fixedName :: Text
  , fixedSize :: Int64
  , fixedNS :: Maybe Text
  , fixedAliases :: Maybe [Text]
  }
  | ENUM
  { enumName :: Text
  , enumSymbols :: [Text]
  , enumNS :: Maybe Text
  , enumAliases :: Maybe [Text]
  , enumDoc :: Maybe Text
  }
  | RECORD
  { recordName :: Text
  , recordFields :: [Field]
  , recordNS :: Maybe Text
  , recordAliases :: Maybe [Text]
  , recordDoc :: Maybe Text
  }
  | REFERENCE Text
  deriving Show

-- | Field Constructor (at the schema level)
data Field = Field
  { fieldName :: Text
  , fieldType :: Schema
  , fieldAliases :: Maybe [Text]
  , fieldDoc :: Maybe Text
  , fieldDefault :: Maybe Avro
  , fieldOrder :: Maybe Order
  } deriving Show

-- | Sort Order
data Order = Ascending | Descending | Ignore
  deriving (Eq, Show)

-- | Avro Values
data Avro
  = NullV
  | BoolV   { boolv   :: Bool       }
  | IntV    { intv    :: Int32      }
  | LongV   { longv   :: Int64      }
  | FloatV  { floatv  :: Float      }
  | DoubleV { doublev :: Double     }
  | BytesV  { bytesv  :: ByteString }
  | StringV { stringv :: Text       }
  | UnionV
  { uIdx :: Int64
  , uVal :: Avro
  }
  | ArrayV     { arrayv  :: [Avro]         }
  | MapV       { mapv    :: [(Text, Avro)] }
  | FixedV
  { fxName :: Text
  , fxVal :: ByteString
  }
  | EnumV
  { eName :: Text
  , eVal  :: Int32
  }
  | RecordV
  { rName   :: Text
  , rFields  :: [(Text,Avro)]
  } deriving (Eq,Show)

-- | Array and Map Values are encoded as a sequence of blocks, each of which declares its number of items,
--   and optionally the number of bytes. If the number of bytes is declared, this allows for more efficient skipping of data.
data BlockLength = BlockLength Int64 (Maybe Int64)

-- | An @AvroPath@ represents a unique path into Avro data. This allows the referencing of specific data within an Avro dataset.
--   This allows advanced techniques like specifying read (skip) and write (block size) policies on arbitrary criteria.
type AvroPath = [AvroPathElement]

-- | An AvroPathElement is an individual branch in a complex Avro value.
data AvroPathElement
  = AvroField Text        -- field name of a Record
  | AvroMapKey Text       -- map key in a map
  | AvroArrayIndex Int64  -- index of an array
  | AvroUnion Int64        -- fully-qualified name for a type in a union
  deriving Eq

instance FromJSON Schema where
  parseJSON s = case s of
    String "null" -> return NULL
    String "boolean" -> return BOOL
    String "int" -> return INT
    String "long" -> return LONG
    String "float" -> return FLOAT
    String "double" -> return DOUBLE
    String "bytes" -> return BYTES
    String "string" -> return STRING
    String ref -> return $ REFERENCE $ fromChunks [ref]
    Array a -> UNION <$> mapM parseJSON (toList a)
    Object o -> do
      t <- o .: "type"
      case t of
        String "record" -> RECORD <$> o .:"name" <*> (o .: "fields" >>= mapM parseJSON) <*> o .:? "namespace" <*> o .:? "aliases" <*> o .:? "doc"
        String "enum" -> ENUM <$> o .: "name" <*> o .: "symbols" <*> o .:? "namespace" <*> o .:? "aliases" <*> o .:? "doc"
        String "array" -> ARRAY <$> o .: "items"
        String "map" -> MAP <$> o .: "values"
        String "fixed" -> FIXED <$> o .: "name" <*> o .: "size" <*> o .:? "namespace" <*> o .:? "aliases"
        String ref -> return $ REFERENCE $ fromChunks [ref]
        _ -> empty
    _ -> empty

-- __TO DO__: implement default values
instance FromJSON Field where
  parseJSON (Object o) = Field <$> o .: "name" <*> o .: "type"  <*> o .:? "aliases" <*> o .:? "doc" <*> pure Nothing <*> o .:? "order"
  parseJSON _ = empty

instance FromJSON Order where
  parseJSON (String s) =  case s of
    "ascending" -> return Ascending
    "descending" -> return Descending
    "ignore" -> return Ignore
    _ -> empty
  parseJSON _ = empty

instance ToJSON Schema where
  toJSON a = case a of
    NULL -> String "null"
    BOOL -> String "boolean"
    INT -> String "int"
    LONG -> String "long"
    FLOAT -> String "float"
    DOUBLE -> String "double"
    BYTES -> String "bytes"
    STRING -> String "string"
    -- __TO DO__: Lazy/Strictness. The name of a named type should never need to exceed chunk size.
    --            In any case, this seems to be a limitation of Aeson.
    REFERENCE ref -> String $ head $ Data.Text.Lazy.toChunks ref
    UNION schemas -> Array $ fromList $ map toJSON schemas
    ARRAY schema -> object ["type".=String "array", "items" .= schema]
    MAP schema -> object ["type".=String "map", "values" .= schema]
    -- __TO DO__: Workaround for fromIntegral?
    FIXED n s ns al -> object $ catMaybes [Just $ "type" .= String "fixed", Just $ "size" .= Number (fromRational $ fromIntegral s), f "namespace" ns, f "aliases" al]
    ENUM n s ns al doc -> object $ catMaybes [Just $ "type" .= String "enum", Just $ "symbols" .= s, f "namespace" ns, f "aliases" al, f "doc" doc]
    RECORD n fs ns al doc -> object $ catMaybes [Just $ "type" .= String "record", Just $ "fields" .= fs, f "namespace" ns, f "aliases" al, f "doc" doc]

instance ToJSON Field where
  toJSON (Field n schema al doc def ord) = object $ catMaybes [Just $ "name".=n, Just $ "type" .= schema, f "doc" doc, f "doc" doc, f "order" ord] -- f "default" def requires ToJSON Avro

instance ToJSON Order where
  toJSON o = case o of
    Ascending -> String "ascending"
    Descending -> String "descending"
    Ignore -> String "ignore"

instance ToJSON Avro where
  toJSON a = case a of
    NullV -> Null
    BoolV v -> Bool v
    -- __TO DO__: Workaround for fromIntegral?
    IntV v -> Number $ fromRational $ fromIntegral v
    LongV v -> Number $ fromRational $ fromIntegral v
    FloatV v -> Number $ fromFloatDigits v
    DoubleV v -> Number $ fromFloatDigits v
    -- __TO DO__: Laziness/Strictness. Confirm correct decoding of bytes. decodeASCII is deprecated
    BytesV v -> String $ decodeUtf8 (head $ Data.ByteString.Lazy.toChunks v)
    StringV v -> String $ head $ Data.Text.Lazy.toChunks v
    UnionV idx v -> toJSON v
    ArrayV vs -> toJSON vs
    -- more lazy/strictness problems
    MapV vs -> object $ map (\(x,y) -> head (Data.Text.Lazy.toChunks x) .= y) vs
    FixedV n v  -> String $ decodeUtf8 $ head (Data.ByteString.Lazy.toChunks v)
    EnumV n v -> String $ head $ Data.Text.Lazy.toChunks n
    RecordV n fs -> object $ map (\(f,a) ->  head (Data.Text.Lazy.toChunks f) .= a) fs

f n = maybe Nothing (\i -> Just $ n.=i)

data Container = Container Schema ByteString [Avro]
  deriving Show

data Compression
  = NullCodec
  | Deflate
  | Snappy
  | UnsupportedCodec
