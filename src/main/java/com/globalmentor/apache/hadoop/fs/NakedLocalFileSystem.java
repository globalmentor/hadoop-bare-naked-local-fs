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

import static java.nio.file.Files.*;

import java.io.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.Set;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;

/**
 * Implementation of the Hadoop {@link FileSystem} API for local file system access directly via the Java API.
 * @apiNote The name of this class is a lighthearted reference to the {@link RawLocalFileSystem} it extends, a play on the Portuguese expression, "a verdade,
 *          nua e crua" ("the raw, naked truth").
 * @implNote This class along with {@link BareLocalFileSystem} mirror and extend the {@link LocalFileSystem}/{@link RawLocalFileSystem} classes, overriding any
 *           shell access and instead directly accessing the Java NIO file system API.
 * @author Garret Wilson
 */
public class NakedLocalFileSystem extends RawLocalFileSystem {

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
	 * {@inheritDoc}
	 * @implSpec This implementation using Java NIO to directly access the {@link PosixFileAttributeView}. If the file system does not support the
	 *           {@link PosixFileAttributeView}, no action is taken.
	 */
	@Override
	public void setPermission(final Path path, final FsPermission permission) throws IOException {
		final PosixFileAttributeView posixFileAttributeView = getFileAttributeView(toNioPath(path), PosixFileAttributeView.class);
		if(posixFileAttributeView != null) {
			posixFileAttributeView.setPermissions(toNioPosixFilePermissions(permission));
		}
	}

}
