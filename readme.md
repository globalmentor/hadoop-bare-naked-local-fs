# GlobalMentor Hadoop Bare Naked Local FileSystem

A Hadoop local `FileSystem` implementation directly accessing the Java API without Winutils, suitable for use with Spark.

_The name of this project refers to the `BareLocalFileSystem` and `NakedLocalFileSystem` classes, and is a lighthearded reference to the Hadoop `RawLocalFileSystem` class which `NakedLocalFileSystem` extends—a play on the Portuguese expression, "a verdade, nua e crua" ("the raw, naked truth")._

## Usage

1. If you have an application that needs Hadoop local `FileSystem` support without relying on Winutils, import the latest [`com.globalmentor:hadoop-bare-naked-local-fs`](https://search.maven.org/artifact/com.globalmentor/hadoop-bare-naked-local-fs) library into your project, e.g. in Maven for v0.1.0:
```xml
<dependency>
  <groupId>com.globalmentor</groupId>
  <artifactId>hadoop-bare-naked-local-fs</artifactId>
  <version>0.1.0</version>
</dependency>
```

2. Then specify that you want to use the Bare Local File System implementation `com.globalmentor.apache.hadoop.fs.BareLocalFileSystem` for the `file` scheme. (`BareLocalFileSystem` internally uses `NakedLocalFileSystem`.)  The following example does this for Spark in Java:
```java
SparkSession spark = SparkSession.builder().appName("Foo Bar").master("local").getOrCreate();
spark.sparkContext().hadoopConfiguration().setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);
```

_Note that you may still get warnings that "HADOOP_HOME and hadoop.home.dir are unset" and "Did not find winutils.exe". This is because the Winutils kludge permeates the Hadoop code and is hard-coded at a low-level, executed statically upon class loading, even for code completely unrelated to file access. See [HADOOP-13223: winutils.exe is a bug nexus and should be killed with an axe.](https://issues.apache.org/jira/browse/HADOOP-13223)_

## Limitations

* The current implementation does not handle symbolic links, but this is planned.
* The current implementation does not register as a service supporting the `file` scheme, and instead must be specified manually. A future version will register with Java's service loading mechanism as `org.apache.hadoop.fs.LocalFileSystem` does, although this will still require manual specification in an environment such as Spark, because there would be two conflicting registered file systems for `file`.
* The current implementation does not support the *nix [sticky bit](https://en.wikipedia.org/wiki/Sticky_bit).

## Background

The [Apache Hadoop](https://hadoop.apache.org/) [`FileSystem`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java) was designed tightly coupled to *unix file systems. It assumes POSIX file permissions. Its [model](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/model.html) definition is still sparse. Little thought was put into creating a general file access API that could be implemented across platforms. File access on non *nix systems such as Windows was largely ignored and few cared.

Unfortunately the Hadoop `FileSystem` API has become somewhat of a de-facto common file storage layer for big data processing, essentially tying big data to *nix systems if local file access is desired. For example, [Apache Spark](https://spark.apache.org/) pulls in Hadoop's `FileSystem` (and the entire Spark client access layer) to write output files to the local file system. Running Spark on Windows, even for prototyping, would be impossible without a Windows implementation of `FileSystem`.

[`RawLocalFileSystem`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/RawLocalFileSystem.java), accessed indirectly via [`LocalFileSystem`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/LocalFileSystem.java), is Hadoop's attempt at Java access of a local file system. It written before Java added access to *nix-centric features such as POSIX file permissions. `RawLocalFileSystem` attempts to access the local file system using system libraries via JNI, and if that is not possible falls back to creating [`Shell`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Shell.java) processes that run *nix commands such as `chmod` or `bash`. This in itself represents a security concern, not to mention an inefficient kludge.

In order to allow `RawLocalFileSystem` to function on Windows (for example to run Spark), one Hadoop contributor created the [`winutils`](https://github.com/steveloughran/winutils) package. This represents a set of binary files that run on Windows and "intercept" the `RawLocalFileSystem` native calls. While the ability to run Spark on Windows was of course welcome, the design represents one more kludge on top of an existing kludge, requiring the trust of more binary distributions and another potential vector for malicious code. (For these reasons there are Hadoop tickets such as [HADOOP-13223: winutils.exe is a bug nexus and should be killed with an axe.](https://issues.apache.org/jira/browse/HADOOP-13223))

This Hadoop Bare Naked Local File System project bypasses Winutils and forces Hadoop to access the file system via pure Java. The `BareLocalFileSystem` and `NakedLocalFileSystem` classes are versions of `LocalFileSystem` and `RawLocalFileSystem`, respectively, which bypass the outdated native and shell access to the local file system and use the Java API instead. It means that projects like Spark can access the file system on Windows as well as on other platforms, without the need to pull in some third-party kludge such as Winutils.

## Implementation Caveats (problems brought by `LocalFileSystem` and `RawLocalFileSystem`)

This Hadoop Bare Naked Local File System implementation extends `LocalFileSystem` and `RawLocalFileSystem` and "reverses" or "undoes" as much as possible JNI and shell access. Much of the original Hadoop kludge implementation is still present beneath the surface (meaning that "Bare Naked" is for the moment a bit of a misnomer).

Unfortunately solving the problem of Hadoop's default local file system accessing isn't as simple as just changing native/shell calls to their modern Java equivalents. The current `LocalFileSystem` and `RawLocalFileSystem` implementations have evolved haphazardly, with halway-implemented features scattered about, special-case code for ill-documented corner cases, and implementation-specific assumptions permeating the design itself. Here are a few examples.

* Tentacled references to Winutils are practically everywhere, even where you least expect it. When Spark starts up, the (shaded) `org.apache.hadoop.security.SecurityUtil` class statically sets up an internal configuration object using `setConfigurationInternal(new Configuration())`. Part of this initialization calls `conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT)`. What could `Configuration.setBoolean()` have to do with Winutils? The (shaded) `org.apache.hadoop.conf.Configuration.getBoolean(String name, boolean defaultValue)` method uses `StringUtils.equalsIgnoreCase("true", valueString)` to convert the String to Boolean, and the (shaded) `org.apache.hadoop.util.StringUtils` class has a static reference to `Shell` in `public static final Pattern ENV_VAR_PATTERN = Shell.WINDOWS ? WIN_ENV_VAR_PATTERN : SHELL_ENV_VAR_PATTERN`. Simply referencing `Shell` brings in the whole static initialization block that looks for Winutils. This is low-level, hard-coded stuff (trash) that represented a bad design to begin with. (These sort of things should be pluggable and configurable, not hard-coded into static initializations.)
```java
    if (WINDOWS) {
      try {
        file = getQualifiedBin(WINUTILS_EXE);
        path = file.getCanonicalPath();
        org.apache.hadoop.shaded.io. = null;
      } catch (IOException e) {
        LOG.warn("Did not find {}: {}", WINUTILS_EXE, e);
        // stack trace org.apache.hadoop.shaded.com.s at debug level
        LOG.debug("Failed to find " + WINUTILS_EXE, e);
        file = null;
        path = null;
        org.apache.hadoop.shaded.io. = e;
      }
    } else {
```
* At the `FileSystem` level Winutils-related logic isn't contained to `RawLocalFileSystem`, which would have allowed it to easily be overridden, but instead relies on the static [`FileUtil`](https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java) class which is like a separate file system implementation that relies on Winutils and can't be modified. For example here is `FileUtil` code that would need to be updated, unfortunately independently of the `FileSystem` implementation:
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
