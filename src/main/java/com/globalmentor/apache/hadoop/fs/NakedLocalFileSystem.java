/*
 * Copyright © 2022 GlobalMentor, Inc. <https://www.globalmentor.com/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.globalmentor.apache.hadoop.fs;

import static java.lang.String.format;
import static java.util.Objects.*;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.stream.Stream;

import javax.annotation.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;

/**
 * Implementation of the Hadoop {@link FileSystem} API for local file system access directly via the Java API.
 * @apiNote The name of this class is a lighthearted reference to the {@link RawLocalFileSystem} it extends, a play on the Portuguese expression, "a verdade,
 *          nua e crua" ("the raw, naked truth").
 * @implSpec This implementation does not support reading or setting the <a href="https://en.wikipedia.org/wiki/Sticky_bit">sticky bit</a>. This implementation
 *           does not yet support symbolic links, but is expected to in the future.
 * @implNote This class along with {@link BareLocalFileSystem} mirror and extend the {@link LocalFileSystem}/{@link RawLocalFileSystem} classes, overriding any
 *           shell access and instead directly accessing the Java NIO file system API.
 * @author Garret Wilson
 */
public class NakedLocalFileSystem extends RawLocalFileSystem {

	/** Internal flag indicating whether the file system is running on the Windows operating system. */
	protected static final boolean IS_WINDOWS_OS = Optional.ofNullable(System.getProperty("os.name")).filter(osName -> osName.startsWith("Windows")).isPresent();

	private long defaultBlockSize;

	/** @return The default block size for this file system. */
	protected long getFileSystemDefaultBlockSize() {
		return defaultBlockSize;
	}

	@Override
	public void initialize(final URI uri, final Configuration conf) throws IOException {
		super.initialize(uri, conf);
		this.defaultBlockSize = getDefaultBlockSize(new Path(uri));
	}

	/**
	 * {@inheritDoc}
	 * @implSpec This implementation returns <code>false</code> because symbolic links are not yet supported.
	 * @implNote It does not appears that the default code in the the parent {@link RawLocalFileSystem} and related classes actually <em>check</em> this indicator
	 *           at the appropriate places, so it is not guaranteed that symlink-related calls will fail-fast with an appropriate error.
	 */
	@Override
	public boolean supportsSymlinks() {
		return false;
	}

	/**
	 * Converts a Hadoop path to a Java NIO path.
	 * @implNote This implementation leverages the existing {@link RawLocalFileSystem#pathToFile(Path)} method. It should be improved to use pure NIO methods,
	 *           obviating the need for an intermediate {@link File} conversion.
	 * @param path The Hadoop path to convert.
	 * @return An equivalent Java NIO path.
	 */
	public java.nio.file.Path toNioPath(final Path path) {
		return pathToFile(path).toPath();
	}

	/**
	 * Converts a Hadoop file system permissions instance to a set of Java NIO POSIX permissions.
	 * @param permission The Hadoop permissions instance.
	 * @return An equivalent set of Java NIO POSIX permissions.
	 */
	public static Set<PosixFilePermission> toNioPosixFilePermissions(final FsPermission permission) {
		final Set<PosixFilePermission> nioPosixFilePermissions = EnumSet.noneOf(PosixFilePermission.class);
		final FsAction userAction = permission.getUserAction();
		if(userAction.implies(FsAction.READ)) {
			nioPosixFilePermissions.add(PosixFilePermission.OWNER_READ);
		}
		if(userAction.implies(FsAction.WRITE)) {
			nioPosixFilePermissions.add(PosixFilePermission.OWNER_WRITE);
		}
		if(userAction.implies(FsAction.EXECUTE)) {
			nioPosixFilePermissions.add(PosixFilePermission.OWNER_EXECUTE);
		}
		final FsAction groupAction = permission.getGroupAction();
		if(groupAction.implies(FsAction.READ)) {
			nioPosixFilePermissions.add(PosixFilePermission.GROUP_READ);
		}
		if(groupAction.implies(FsAction.WRITE)) {
			nioPosixFilePermissions.add(PosixFilePermission.GROUP_WRITE);
		}
		if(groupAction.implies(FsAction.EXECUTE)) {
			nioPosixFilePermissions.add(PosixFilePermission.GROUP_EXECUTE);
		}
		final FsAction otherAction = permission.getOtherAction();
		if(otherAction.implies(FsAction.READ)) {
			nioPosixFilePermissions.add(PosixFilePermission.OTHERS_READ);
		}
		if(otherAction.implies(FsAction.WRITE)) {
			nioPosixFilePermissions.add(PosixFilePermission.OTHERS_WRITE);
		}
		if(otherAction.implies(FsAction.EXECUTE)) {
			nioPosixFilePermissions.add(PosixFilePermission.OTHERS_EXECUTE);
		}
		return nioPosixFilePermissions;
	}

	/**
	 * Converts a set of Java NIO POSIX permissions to áHadoop file system permissions instance.
	 * @implNote This could be more efficiently implemented by using each permission present as a bit shifted to the enum ordinal (after subtracting to ensure the
	 *           correct bit order) and ORing to form a single bode for constructing an {@link FsPermission}.
	 * @param nioPosixFilePermissions The set of Java NIO POSIX permissions.
	 * @return An equivalent Hadoop permissions instance.
	 */
	public static FsPermission toFsPermission(final Set<PosixFilePermission> nioPosixFilePermissions) {
		final FsAction ownerAction = fsActionOf(nioPosixFilePermissions.contains(PosixFilePermission.OWNER_READ),
				nioPosixFilePermissions.contains(PosixFilePermission.OWNER_WRITE), nioPosixFilePermissions.contains(PosixFilePermission.OWNER_EXECUTE));
		final FsAction groupAction = fsActionOf(nioPosixFilePermissions.contains(PosixFilePermission.GROUP_READ),
				nioPosixFilePermissions.contains(PosixFilePermission.GROUP_WRITE), nioPosixFilePermissions.contains(PosixFilePermission.GROUP_EXECUTE));
		final FsAction otherAction = fsActionOf(nioPosixFilePermissions.contains(PosixFilePermission.OTHERS_READ),
				nioPosixFilePermissions.contains(PosixFilePermission.OTHERS_WRITE), nioPosixFilePermissions.contains(PosixFilePermission.OTHERS_EXECUTE));
		return new FsPermission(ownerAction, groupAction, otherAction);
	}

	/**
	 * Factory method to create a single Hadoop permission action based upon read, write, and/or execute permissions.
	 * @param read Whether read permission should be included.
	 * @param write Whether write permission should be included.
	 * @param execute Whether execute permission should be included.
	 * @return The Hadoop permission action reflecting the requested permissions.
	 */
	public static FsAction fsActionOf(final boolean read, final boolean write, final boolean execute) {
		FsAction result = FsAction.NONE;
		if(read) {
			result = result.or(FsAction.READ);
		}
		if(write) {
			result = result.or(FsAction.WRITE);
		}
		if(execute) {
			result = result.or(FsAction.EXECUTE);
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @implSpec This implementation using Java NIO to directly access the {@link PosixFileAttributeView}. If the file system does not support the
	 *           {@link PosixFileAttributeView}, no action is taken.
	 */
	@Override
	public void setPermission(final Path path, final FsPermission permission) throws IOException {
		final PosixFileAttributeView posixFileAttributeView = Files.getFileAttributeView(toNioPath(path), PosixFileAttributeView.class);
		if(posixFileAttributeView != null) {
			posixFileAttributeView.setPermissions(toNioPosixFilePermissions(permission));
		}
	}

	/**
	 * {@inheritDoc}
	 * @implSpec This implementation delegates to {@link #listStatus(java.nio.file.Path)}.
	 */
	@Override
	public FileStatus[] listStatus(final Path path) throws IOException {
		return listStatus(toNioPath(path));
	}

	/**
	 * List the statuses of the files/directories in the given path if the path is a directory; otherwise returns an array containing the status of the path
	 * itself. The statuses are not guaranteed to be returned in any particular order.
	 * @param nioPath The Java NIO path for which to list directories.
	 * @return The statuses of the files/directories in the given path.
	 * @throws FileNotFoundException when the path does not exist.
	 * @throws IOException If a general I/O exception occurs.
	 */
	public FileStatus[] listStatus(final java.nio.file.Path nioPath) throws FileNotFoundException, IOException {
		if(!Files.isDirectory(nioPath, LinkOption.NOFOLLOW_LINKS)) { //if this is not a directory (and not _supposed_ to be a directory, so don't follow symlinks)
			return new FileStatus[] {getFileStatus(nioPath)}; //return non-directories as a single list of the single file
		}

		//no need to check if path exists --- listing its contents will do this automatically anyway
		try (final Stream<java.nio.file.Path> childNioPaths = Files.list(nioPath)) {
			return childNioPaths.map(childNioPath -> {
				try {
					return getFileStatus(childNioPath);
				} catch(final FileNotFoundException fileNotFoundException) {
					//If a child disappears before we can describe it, don't consider that an error;
					//instead, consider that the directory listing has changed. (This implies that the
					//exact static directory list returned might never have existed at any point in time.)
					return null;
				} catch(final IOException ioException) {
					throw new UncheckedIOException(ioException); //another approach would be to use FauxPas or similar
				}
			}).filter(Objects::nonNull).toArray(FileStatus[]::new);
		} catch(final UncheckedIOException uncheckedIOException) {
			throw uncheckedIOException.getCause();
		}
	}

	/**
	 * {@inheritDoc}
	 * @implSpec This method delegates to {@link #getFileStatus(java.nio.file.Path)}.
	 */
	@Override
	public FileStatus getFileStatus(final Path path) throws IOException {
		return getFileStatus(toNioPath(path));
	}

	/**
	 * Returns a file status object that represents the Java NIO path.
	 * @param nioPath The Java NIO path path we want information from.
	 * @return A file status instance describing the file.
	 * @throws FileNotFoundException if the path does not exist.
	 * @throws IOException If a general I/O exception occurs.
	 */
	public FileStatus getFileStatus(final java.nio.file.Path nioPath) throws IOException {
		try {
			//no need to check if path exists --- reading its attributes will do this automatically anyway
			final BasicFileAttributes nioBasicFileAttributes = Files.readAttributes(nioPath, BasicFileAttributes.class);
			PosixFileAttributes nioPosixFileAttributes;
			try {
				nioPosixFileAttributes = Files.readAttributes(nioPath, PosixFileAttributes.class);
			} catch(final UnsupportedOperationException unsupportedOperationException) {
				nioPosixFileAttributes = null;
			}
			return new NakedLocalFileStatus(nioPath, getFileSystemDefaultBlockSize(), nioBasicFileAttributes, nioPosixFileAttributes, this);
		} catch(final NoSuchFileException noSuchFileException) {
			throw (FileNotFoundException)new FileNotFoundException(format("File `%s` does not exist.", nioPath)).initCause(noSuchFileException);
		}
	}

	@Override
	public String toString() {
		return "NakedLocalFS";
	}

	/**
	 * Java NIO file status implementation.
	 * @implNote This class duplicates much code from the package-private (and thus inaccessible here)
	 *           <code>RawLocalFileSystem.DeprecatedRawLocalFileStatus</code>, which according to the comments apparently at one time was intended to go away in
	 *           favor of shell-based and JNI access to the file system, but was retained indefinitely because of
	 *           <a href="https://issues.apache.org/jira/browse/HADOOP-9652">HADOOP-9652</a>.
	 * @author Garret Wilson
	 */
	protected static class NakedLocalFileStatus extends FileStatus {

		private static final long serialVersionUID = 1L;

		/** The Java NIO path this file status represents. */
		private final java.nio.file.Path nioPath;

		/** @return The Java NIO path this file status represents. */
		public java.nio.file.Path getNioPath() {
			return nioPath;
		}

		/**
		 * Normal file and directory constructor. Symbolic links are followed.
		 * @apiNote The constructor throws the Java NIO {@link NoSuchFileException} if the file is missing at some point during construction. It should be converted
		 *          later to the {@link FileNotFoundException} used by the Hadoop file system API.
		 * @param nioPath The Java NIO path to the file.
		 * @param blockSize The block size to use.
		 * @param nioBasicFileAttributes The Java NIO basic file attributes.
		 * @param nioPosixFileAttributes The Java NIO POXIS file attributes, or <code>null</code> if not available on this platform.
		 * @param fileSystem The file system requesting the file status.
		 * @throws NoSuchFileException if the file is missing or goes missing while the status information is being retrieved.
		 * @throws IOException If a general I/O exception occurs.
		 */
		public NakedLocalFileStatus(final java.nio.file.Path nioPath, long blockSize, final BasicFileAttributes nioBasicFileAttributes,
				@Nullable final PosixFileAttributes nioPosixFileAttributes, final FileSystem fileSystem) throws IOException {
			super(Files.size(nioPath), Files.isDirectory(nioPath), 1, blockSize, nioBasicFileAttributes.lastModifiedTime().toMillis(),
					nioBasicFileAttributes.lastAccessTime().toMillis(), nioPosixFileAttributes != null ? toFsPermission(nioPosixFileAttributes.permissions()) : null,
					nioPosixFileAttributes != null ? removePrincipalDomainIfWindows(nioPosixFileAttributes.owner().getName()) : null,
					nioPosixFileAttributes != null ? removePrincipalDomainIfWindows(nioPosixFileAttributes.group().getName()) : null,
					new Path(nioPath.normalize().toString()).makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory()));
			this.nioPath = requireNonNull(nioPath);
		}

		/** The separator string used by Windows to separate the domain name from the principal name for the user or group identifier. */
		private static final String WINDOWS_PRINCIPAL_DOMAIN_NAME_SEPARATOR = "\\";

		/**
		 * If on the Windows platform, removes any domain name from a Windows security principal in the form <code>&lt;domain&gt;\\user</code> or
		 * <code>&lt;domain&gt;\\group</code>, yielding simply <code>user</code> or <code>group</code>, respectively.
		 * @param principalName The full user or group name.
		 * @return The user or group ID without the domain portion.
		 * @see NakedLocalFileSystem#IS_WINDOWS_OS
		 * @see #WINDOWS_PRINCIPAL_DOMAIN_NAME_SEPARATOR
		 */
		private static String removePrincipalDomainIfWindows(String principalName) {
			if(IS_WINDOWS_OS) {
				final int index = principalName.indexOf(WINDOWS_PRINCIPAL_DOMAIN_NAME_SEPARATOR);
				if(index != -1) {
					principalName = principalName.substring(index + 1);
				}
			}
			return principalName;
		}

	}

}
