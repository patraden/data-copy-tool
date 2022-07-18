package dct

/** Super class for all Data-Copy-Tool exceptions */
class DataCopyException(message: String, cause: Throwable = null) extends Exception(message, cause)
/** Streaming initialization stage exception */
class StreamInitializationException(message: String, cause: Throwable = null) extends DataCopyException(message, cause)
/** Streaming stage exception */
class StreamException(message: String, cause: Throwable = null) extends DataCopyException(message, cause)
/** Streaming stage exception */
class StreamMetaDataAssertionException(actual: String, expected: String)
  extends StreamException(s"Actual $actual metadata value is not equal to expected: $expected")
/** Streaming wrap up stage exception */
class StreamWrappingException(message: String, cause: Throwable) extends DataCopyException(message, cause)
/** Streaming wrap up stage exception */
class CleanTargetException(message: String, cause: Throwable) extends StreamWrappingException(message: String, cause: Throwable)
/** Streaming wrap up stage exception */
class RenameTempToTargetException(message: String, cause: Throwable) extends StreamWrappingException(message: String, cause: Throwable)
/** Streaming rollback exception */
class StreamRollbackException(message: String, cause: Throwable) extends DataCopyException(message, cause)
