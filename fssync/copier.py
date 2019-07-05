from fs.bulk import Copier as fsCopier
import time


def copy_file_data(src_file, dst_file, chunk_size=None, callback=None):
    # type: (IO, IO, Optional[int]) -> None
    """Copy data from one file object to another.

    Arguments:
        src_file (io.IOBase): File open for reading.
        dst_file (io.IOBase): File open for writing.
        chunk_size (int): Number of bytes to copy at
            a time (or `None` to use sensible default).

    """
    _chunk_size = 1024 * 1024 if chunk_size is None else chunk_size
    read = src_file.read
    write = dst_file.write
    # The 'or None' is so that it works with binary and text files
    for chunk in iter(lambda: read(_chunk_size) or None, None):
        write(chunk)
        callback(len(chunk))


def do_nothing(a):
    pass


class CountCallback(object):

    __slots__ = ('total', 'count', 'timeit', 'timeitold', 'speedd', 'callback', 'src_fs', 'dst_fs', 'src', 'dest')

    def __init__(self, total, src, dest, callback=None):
        self.src_fs = None
        self.dst_fs = None
        self.total = total
        self.count = 0
        self.timeit = time.time()
        self.timeitold = 0
        self.speedd = 0
        self.src = src
        self.dest = dest
        if callback:
            self.callback = callback
        else:
            self.callback = do_nothing

    @property
    def percent(self):
        return (self.count/self.total)*100

    @property
    def finish(self):
        return self.count == self.total

    @property
    def speed(self):
        return self.speedd

    def __call__(self, chunk):
        self.count += chunk
        t1 = self.timeitold
        t2 = self.timeit
        t1 = t2
        t2 = time.time()
        self.speedd = chunk/(t2-t1)
        self.callback(self)


class _CopyTask(object):
    """A callable that copies from one file another."""
    def __init__(self, src_file, dst_file, callback=None):
        self.src_file = src_file
        self.dst_file = dst_file
        self.callback = callback

    def __repr__(self):
        return 'CopyTask(%r, %r)'.format(
            self.src_file,
            self.dst_file,
        )

    def __call__(self):
        try:
            copy_file_data(
                self.src_file, self.dst_file, chunk_size=1024 * 1024,
                callback=self.callback
            )
        except Exception as e:
            print(e)
        finally:
            try:
                self.src_file.close()
            finally:
                self.dst_file.close()


class Copier(fsCopier):

    def copy(self, src_fs, src_path, dst_fs, dst_path, callback, inject_fs=False):
        """Copy a file from on fs to another.
            callback reciev a CountCallback object."""
        src_file = src_fs.openbin(src_path, 'r')
        size = src_fs.getinfo(src_path, namespaces=['details']).size
        callbc = CountCallback(size, src_path, dst_path,  callback)
        if inject_fs:
            callbc.src_fs = src_fs
            callbc.dst_fs = dst_fs
        try:
            dst_file = dst_fs.openbin(dst_path, 'w')
        except Exception as e:
            # If dst file fails to open, explicitly close src_file
            src_file.close()
            print(e)
            raise
        task = _CopyTask(src_file, dst_file, callbc)
        if self.num_workers:
            self.queue.put(task)
        else:
            task()