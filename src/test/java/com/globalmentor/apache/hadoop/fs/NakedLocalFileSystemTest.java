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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;

import org.apache.hadoop.fs.permission.*;
import org.junit.jupiter.api.Test;

/**
 * Unit tests of {@link NakedLocalFileSystem}.
 * @author Garret Wilson
 */
public class NakedLocalFileSystemTest {

	@Test
	void testToNioPosixFilePermissions() {
		assertThat(NakedLocalFileSystem.toNioPosixFilePermissions(new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE)),
				is(EnumSet.noneOf(PosixFilePermission.class)));
		assertThat(NakedLocalFileSystem.toNioPosixFilePermissions(new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ)),
				is(EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ,
						PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ)));
		assertThat(NakedLocalFileSystem.toNioPosixFilePermissions(new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)),
				is(EnumSet.allOf(PosixFilePermission.class)));
	}

}
