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

import static java.nio.charset.StandardCharsets.*;
import static java.nio.file.Files.*;
import static java.util.Arrays.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests of {@link NakedLocalFileSystem}.
 * @author Garret Wilson
 */
public class NakedLocalFileSystemIT {

	private NakedLocalFileSystem testFileSystem;

	@BeforeEach
	void setupFileSystem() {
		testFileSystem = new NakedLocalFileSystem();
	}

	@AfterEach
	void teardownFileSystem() throws IOException {
		testFileSystem.close();
	}

	/** @see NakedLocalFileSystem#getFileStatus(java.nio.file.Path) */
	@Test
	void testGetFileStatusThrowsFileNotFoundExceptionForMissingFile(@TempDir final java.nio.file.Path tempDir) throws IOException {
		assertThrows(FileNotFoundException.class, () -> testFileSystem.getFileStatus(tempDir.resolve("missing")));
	}

	/** @see NakedLocalFileSystem#getFileStatus(java.nio.file.Path) */
	@Test
	void testGetFileStatusForFile(@TempDir final java.nio.file.Path tempDir) throws IOException {
		final java.nio.file.Path testTextFile = write(tempDir.resolve("test.txt"), "foobar".getBytes(UTF_8)); //`/test.txt`: "foobar"
		final FileStatus testFileStatus = testFileSystem.getFileStatus(testTextFile);
		assertThat(testFileStatus.getBlockSize(), is(testFileSystem.getFileSystemDefaultBlockSize()));
		assertThat(testFileStatus.isFile(), is(true));
		assertThat(testFileStatus.isDirectory(), is(false));
		assertThat(testFileStatus.getModificationTime(), is(getLastModifiedTime(testTextFile).toMillis()));
	}

	/** @see NakedLocalFileSystem#getFileStatus(java.nio.file.Path) */
	@Test
	void testGetFileStatusForDirectory(@TempDir final java.nio.file.Path tempDir) throws IOException {
		final java.nio.file.Path foobarDirectory = createDirectory(tempDir.resolve("foobar")); //`/foobar/`
		final FileStatus testFileStatus = testFileSystem.getFileStatus(foobarDirectory);
		assertThat(testFileStatus.getBlockSize(), is(testFileSystem.getFileSystemDefaultBlockSize()));
		assertThat(testFileStatus.isFile(), is(false));
		assertThat(testFileStatus.isDirectory(), is(true));
	}

	/** @see NakedLocalFileSystem#listStatus(java.nio.file.Path) */
	@Test
	void testListStatusForDirectory(@TempDir final java.nio.file.Path tempDir) throws IOException {
		final java.nio.file.Path fooFile = write(tempDir.resolve("foo.txt"), "foo".getBytes(UTF_8)); //`/foo.txt`: "foo"
		final java.nio.file.Path barFile = write(tempDir.resolve("bar.txt"), "bar".getBytes(UTF_8)); //`/bar.txt`: "bar"
		final java.nio.file.Path foobarDirectory = createDirectory(tempDir.resolve("foobar")); //`/foobar/`
		assertThat(testFileSystem.listStatus(tempDir),
				arrayContainingInAnyOrder(testFileSystem.getFileStatus(fooFile), testFileSystem.getFileStatus(barFile), testFileSystem.getFileStatus(foobarDirectory)));
		assertThat((Iterable<String>)asList(testFileSystem.listStatus(tempDir)).stream().map(FileStatus::getPath).map(Path::getName)::iterator,
				containsInAnyOrder("foo.txt", "bar.txt", "foobar"));
	}

}
