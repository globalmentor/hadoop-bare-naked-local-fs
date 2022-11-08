# GlobalMentor Hadoop Bare Naked Local FileSystem

A Hadoop local `FileSystem` implementation directly accessing the Java API without Winutils, suitable for use with Spark.

_The name of this project refers to the `BareLocalFileSystem` and `NakedLocalFileSystem` classes, and is a lighthearded reference to the Hadoop `RawLocalFileSystem` class which `NakedLocalFileSystem` extends—a play on the Portuguese expression, "a verdade, nua e crua" ("the raw, naked truth")._

## Background

The [Apache Hadoop](https://hadoop.apache.org/) [`FileSystem`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java) was designed tightly coupled to *unix file systems. It assumes of POSIX file permissions. Its [model](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/model.html) definition is still sparse. Little thought was put into creating a general file access API that could be implemented across platforms. File access on non *nix systems such as Windows was largely ignored and few cared.

Unfortunately the Hadoop `FileSystem` API has become somewhat of a de-facto common file storage layer for big data processing, essentially tying big data to *nix systems if local file access is desired. For example, [Apache Spark](https://spark.apache.org/) pulls in Hadoop's `FileSystem` (and the entire Spark client access layer) to write output files to the local file system. Running Spark on Windows, even for prototyping, would be impossible without a Windows implementation of `FileSystem`.

[`RawLocalFileSystem`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/RawLocalFileSystem.java), accessed indirectly via [`LocalFileSystem`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/LocalFileSystem.java), is Hadoop's attempt at Java access of a local file system. It written before Java added access to *nix-centric features such as POSIX file permissions. `RawLocalFileSystem` attempts to access the local file system using system libraries via JNI, and if that is not possible falls back to creating [`Shell`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Shell.java) processes that run *nix commands such as `chmod` or `bash`. This in itself represents a security concern, not to mention an inefficient kludge.

In order to allow `RawLocalFileSystem` to function on Windows (for example to run Spark), one Hadoop contributor created the [`winutils`](https://github.com/steveloughran/winutils) package. This represents a set of binary files that run on Windows and "intercept" the `RawLocalFileSystem` native calls. While the ability to run Spark on Windows was of course welcome, the design represents one more kludge on top of an existing kludge, requiring the trust of more binary distributions and another potential vector for malicious code. (For these reasons there are Hadoop tickets such as [HADOOP-13223: winutils.exe is a bug nexus and should be killed with an axe.](https://issues.apache.org/jira/browse/HADOOP-13223))

This Hadoop Bare Naked Local File System project bypasses winutils and forces Hadoop to access the file system via pure Java. The `BareLocalFileSystem` and `NakedLocalFileSystem` classes are versions of `LocalFileSystem` and `RawLocalFileSystem`, respectively, which bypass the outdated native and shell access to the local file system and use the Java API instead. It means that projects like Spark can access the file system on Windows as well as on other platforms, without the need to pull in some third-party kludge such as Winutils.

## Limitations

* The current implementation does not handle symbolic links, but this is planned.
* The current implementation does not support the *nix [sticky bit](https://en.wikipedia.org/wiki/Sticky_bit).

## Implementation Caveats (problems brought by `LocalFileSystem` and `RawLocalFileSystem`)

This Hadoop Bare Naked Local File System implementation extends `LocalFileSystem` and `RawLocalFileSystem` and "reverses" or "undoes" as much as possible JNI and shell access. Much of the original Hadoop kludge implementation is still present beneath the seurface (meaning that "Bare Naked" is for the moment a bit of a misnomer).

Unfortunately solving the problem of Hadoop's default local file system accessing isn't as simple as just changing native/shell calls to their modern Java equivalents. The current `LocalFileSystem` and `RawLocalFileSystem` implementations have evolved haphazardly, with halway-implemented features scattered about, special-case code for ill-documented corner cases, and implementation-specific assumptions permeating the design itself. Here are a few examples.

* Winutils-related logic isn't contained to `RawLocalFileSystem`, allowing it to easily be overridden, but instead relies on the static [`FileUtil`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java) class which is like a separate file system implementation that relies on Winutils and can't be modified. For example here is `FileUtil` code that would need to be updated, unfortunately independently of the `FileSystem` implementation:
```java
  public static String readLink(File f) {
    /* NB: Use readSymbolicLink in java.nio.file.Path once available. Could
     * use getCanonicalPath in File to get the target of the symlink but that
     * does not indicate if the given path refers to a symlink.
     */
    …
    try {
      return Shell.execCommand(
          Shell.getReadlinkCommand(f.toString())).trim();
    } catch (IOException x) {
      return "";
    }
```
* Apparently there is a "new [`Stat`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Stat.java) based implementation" of many methods, but `RawLocalFileSystem` instead uses a deprecated implementations such as `DeprecatedRawLocalFileStatus`, which is full of workarounds and special-cases, is package-private so it can't be accessed by subclasses, yet can't be removed because of [HADOOP-9652](https://issues.apache.org/jira/browse/HADOOP-9652). The `useDeprecatedFileStatus` switch is hard-coded so that it can't be modified by a subclass, forcing a re-implementation of everything it touches. In other words, even the new, less-kludgey approach is switched off in the code, has been for years, and no one seems to be paying it any mind.
* `DeprecatedRawLocalFileStatus` tries to detect if permissions have already been loaded for a file. It checks for an empty string owner; in some conditions (a shell error) the owner is set to `null`, which under the covers actually sets the value to `""`, making the whole process brittle. Moreover an error condition would cause an endless cycle of attempting to load permissions. (And what is `CopyFiles.FilePair()`, and does the current implementation break it, or would it only be broken if "extra fields" are added?)
```java
		/* We can add extra fields here. It breaks at least CopyFiles.FilePair().
		 * We recognize if the information is already loaded by check if
		 * onwer.equals("").
		 */
		private boolean isPermissionLoaded() {
			return !super.getOwner().isEmpty();
		}
```
