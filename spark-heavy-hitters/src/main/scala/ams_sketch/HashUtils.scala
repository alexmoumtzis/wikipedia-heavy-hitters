package ams_sketch

/** Shared MurmurHash-based hashing utilities for AMS sketching. */
object HashUtils {

  /** MurmurHash64 for Long values; `seed` selects the hash family. */
  def murmurHash64(k: Long, seed: Long): Long = {
    val m = 0xc6a4a7935bd1e995L
    val r = 47

    var h = seed ^ (8L * m)
    var x = k

    x *= m
    x ^= x >>> r
    x *= m
    h ^= x
    h *= m

    h ^= h >>> r
    h *= m
    h ^= h >>> r

    h
  }

  /** Hash a String to a stable Long index (deterministic across JVMs). */
  def hashString(value: String): Long = {
    val bytes = value.getBytes("UTF-8")
    var h = 0xcafebabe00000000L

    var i = 0
    while (i < bytes.length) {
      h ^= (bytes(i).toLong & 0xffL) << ((i % 8) * 8)
      h = java.lang.Long.rotateLeft(h, 31)
      h *= 0x9e3779b97f4a7c15L
      i += 1
    }

    h ^= h >>> 33
    h *= 0xff51afd7ed558ccdL
    h ^= h >>> 33
    h *= 0xc4ceb9fe1a85ec53L
    h ^= h >>> 33
    h
  }
}
