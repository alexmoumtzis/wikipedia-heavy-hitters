package common

import java.nio.file.{Path, Paths}

/**
 * Central resolver for the repository's data/results locations so the codebase
 * contains no machine-specific absolute paths.
 *
 * The repository root is resolved in this order:
 *   1. `-Dwiki.hh.home=<path>` JVM system property, or
 *   2. `WIKI_HH_HOME` environment variable, or
 *   3. the current working directory (its parent when launched from the
 *      `spark-heavy-hitters` module directory, e.g. via `sbt`).
 *
 * All public helpers take a path relative to the repository root.
 */
object ProjectPaths {

  val repoRoot: Path = {
    val overridden = Option(System.getProperty("wiki.hh.home"))
      .orElse(Option(System.getenv("WIKI_HH_HOME")))
      .filter(_.nonEmpty)
    val base = overridden match {
      case Some(p) => Paths.get(p)
      case None =>
        val cwd = Paths.get("").toAbsolutePath.normalize
        if (cwd.getFileName != null && cwd.getFileName.toString == "spark-heavy-hitters")
          cwd.getParent
        else cwd
    }
    base.toAbsolutePath.normalize
  }

  /** Absolute path (forward slashes) for a repository-relative location. */
  def path(relative: String): String =
    repoRoot.resolve(relative).toString.replace('\\', '/')

  /** `file:///` URI for a repository-relative location (for Spark/Parquet I/O). */
  def uri(relative: String): String =
    Paths.get(path(relative)).toUri.toString

  def ensureDirectory(directory: String): Unit =
    java.nio.file.Files.createDirectories(Paths.get(directory))
}
