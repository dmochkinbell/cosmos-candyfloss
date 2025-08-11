package com.swisscom.daisy.cosmos.candyfloss.messages;

/**
 * A sealed interface representing all possible output value types. The 'permits' clause explicitly
 * lists all allowed implementations, which must now exist in their own files.
 */
public sealed interface OutputValue permits JsonOutputValue, AvroOutputValue {}
