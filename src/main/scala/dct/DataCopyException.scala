package dct

/* Super class for all Data Copy Tool exceptions */
class DataCopyException(message: String, cause: Throwable = null) extends Exception(message, cause)

/* Streaming initialization stage exceptions */
class StreamInitializationException(message: String, cause: Throwable = null) extends DataCopyException(message, cause)

/* Streaming stage exceptions */
class StreamException(message: String, cause: Throwable = null) extends DataCopyException(message, cause)
class StreamMetaDataAssertionException(actual: String, expected: String)
  extends StreamException(s"Actual $actual metadata value is not equal to expected: $expected")

/* Streaming wrap up stage exceptions */
class StreamWrappingException(message: String, cause: Throwable) extends DataCopyException(message, cause)
class CleanTargetException(message: String, cause: Throwable) extends StreamWrappingException(message: String, cause: Throwable)
class RenameTempToTargetException(message: String, cause: Throwable) extends StreamWrappingException(message: String, cause: Throwable)

/* Streaming rollback exceptions */
class StreamRollbackException(message: String, cause: Throwable) extends DataCopyException(message, cause)
