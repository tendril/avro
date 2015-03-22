module Data.Avro
( Schema(..), Field(..), Order(..), Avro(..), BlockLength(..), AvroPath, AvroPathElement(..) )
where

import Data.Text.Lazy (Text)
import Data.Int
import Data.ByteString.Lazy (ByteString)

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
  , enumDoc :: Maybe [Text]
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
