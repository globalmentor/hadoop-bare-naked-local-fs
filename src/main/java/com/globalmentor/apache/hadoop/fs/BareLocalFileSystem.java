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

import org.apache.hadoop.fs.*;

/**
 * Implementation of the Hadoop {@link FileSystem} API for the checksummed local file system.
 * @implSpec This implementation creates an instance of {@link NakedLocalFileSystem} to serve as the decorated raw local file system.
 * @implNote This class along with {@link NakedLocalFileSystem} mirror and extend the {@link LocalFileSystem}/{@link RawLocalFileSystem} classes, overriding any
 *           shell access and instead directly accessing the Java NIO file system API.
 * @author Garret Wilson
 */
public class BareLocalFileSystem extends LocalFileSystem {

	/**
	 * No-args constructor.
	 * @implSpec This implementation creates and decorates an instance of {@link NakedLocalFileSystem}.
	 */
	public BareLocalFileSystem() {
		this(new NakedLocalFileSystem());
	}

	/**
	 * Decorated raw local file system constructor.
	 * @param rawLocalFileSystem The raw local file system to decorate.
	 */
	protected BareLocalFileSystem(final FileSystem rawLocalFileSystem) {
		super(rawLocalFileSystem);
	}

}
