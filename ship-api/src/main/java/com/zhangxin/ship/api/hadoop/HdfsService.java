package com.zhangxin.ship.api.hadoop;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HdfsService {
    private final Configuration configuration;
    private final String hdfsUri;

    public HdfsService(Configuration configuration, String hdfsUri) {
        this.configuration = configuration;
        this.hdfsUri = hdfsUri;
    }

    private FileSystem getFileSystem() throws IOException {
        return FileSystem.get(configuration);
    }

    public void close(FileSystem fileSystem) {
        if (null != fileSystem) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                log.error("file system close", e);
            }
        }
    }

    /**
     * 将相对路径转化为HDFS文件路径
     */
    private String generateHdfsPath(String path) {
        String hdfsPath = hdfsUri;
        if (path.startsWith("/")) {
            hdfsPath += path;
        } else {
            hdfsPath = hdfsUri + "/" + path;
        }
        return hdfsPath;
    }

    public boolean checkExists(String path) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            String hdfsPath = generateHdfsPath(path);
            return fileSystem.exists(new Path(hdfsPath));
        } catch (IOException e) {
            log.error("check path", e);
        } finally {
            close(fileSystem);
        }
        return false;
    }

    /**
     * 创建HDFS目录
     */
    public boolean mkdir(String path) {
        if (checkExists(path)) {
            return true;
        } else {
            FileSystem fileSystem = null;
            try {
                fileSystem = getFileSystem();
                String hdfsPath = generateHdfsPath(path);
                return fileSystem.mkdirs(new Path(hdfsPath));
            } catch (IOException e) {
                log.error("mkdir path", e);
            } finally {
                close(fileSystem);
            }
            return false;
        }
    }

    /**
     * 重命名
     *
     * @param srcFile 重命名之前的HDFS的相对目录路径，比如：/testDir/b.txt
     * @param dstFile 重命名之后的HDFS的相对目录路径，比如：/testDir/b_new.txt
     */
    public boolean rename(String srcFile, String dstFile) {
        Path srcFilePath = new Path(generateHdfsPath(srcFile));
        Path dstFilePath = new Path(dstFile);
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            return fileSystem.rename(srcFilePath, dstFilePath);
        } catch (IOException e) {
            log.error("rename file", e);
        } finally {
            close(fileSystem);
        }
        return false;
    }

    /**
     * 删除HDFS目录或文件
     */
    public boolean delete(String path) {
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            return fileSystem.delete(hdfsPath, true);
        } catch (IOException e) {
            log.error("delete path", e);
        } finally {
            close(fileSystem);
        }
        return false;
    }

    /**
     * 上传文件至HDFS
     *
     * @param delSrc    是否删除本地文件
     * @param overwrite 是否覆盖HDFS上面的文件
     * @param srcFile   本地文件路径，比如：D:/test.txt
     * @param dstPath   HDFS的相对目录路径，比如：/testDir
     */
    public void upload(boolean delSrc, boolean overwrite, String srcFile, String dstPath) {
        Path localSrcPath = new Path(srcFile);
        Path hdfsDstPath = new Path(generateHdfsPath(dstPath));
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            fileSystem.copyFromLocalFile(delSrc, overwrite, localSrcPath, hdfsDstPath);
        } catch (IOException e) {
            log.error("upload file", e);
        } finally {
            close(fileSystem);
        }
    }

    /**
     * 从HDFS下载文件至本地
     *
     * @param srcFile HDFS的相对目录路径，比如：/testDir/a.txt
     * @param dstFile 下载之后本地文件路径（如果本地文件目录不存在，则会自动创建），比如：D:/test.txt
     */
    public void download(String srcFile, String dstFile) {
        Path hdfsSrcPath = new Path(generateHdfsPath(srcFile));
        Path localDstPath = new Path(dstFile);
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            fileSystem.copyToLocalFile(hdfsSrcPath, localDstPath);
        } catch (IOException e) {
            log.error("download file", e);
        } finally {
            close(fileSystem);
        }
    }

    /**
     * 获取HDFS某个路径下的所有文件或目录（不包含子目录）信息
     *
     * @param path HDFS的相对目录路径，比如：/testDir
     */
    public List<Map<String, Object>> getFiles(String path, PathFilter pathFilter) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (checkExists(path)) {
            FileSystem fileSystem = null;
            try {
                fileSystem = getFileSystem();
                String hdfsPath = generateHdfsPath(path);
                FileStatus[] statuses;
                if (pathFilter != null) {
                    statuses = fileSystem.listStatus(new Path(hdfsPath), pathFilter);
                } else {
                    statuses = fileSystem.listStatus(new Path(hdfsPath));
                }
                if (statuses != null) {
                    for (FileStatus status : statuses) {
                        Map<String, Object> fileMap = new HashMap<>(2);
                        fileMap.put("path", status.getPath().toString());
                        fileMap.put("isDir", status.isDirectory());
                        result.add(fileMap);
                    }
                }
            } catch (IOException e) {
                log.error("get files", e);
            } finally {
                close(fileSystem);
            }
        }
        return result;
    }

    /**
     * 获取某个文件在HDFS集群的位置
     *
     * @param path HDFS的相对目录路径，比如：/testDir/a.txt
     */
    public BlockLocation[] getFileBlockLocations(String path) {
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);
            return fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        } catch (IOException e) {
            log.error("get file location", e);
        } finally {
            close(fileSystem);
        }
        return null;
    }

    /**
     * 打开HDFS文件并返回 InputStream
     *
     * @param path HDFS的相对目录路径，比如：/testDir/c.txt
     */
    public FSDataInputStream open(String path) {
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem;
        try {
            fileSystem = getFileSystem();
            return fileSystem.open(hdfsPath);
        } catch (IOException e) {
            log.error("open file", e);
        }
        return null;
    }

    /**
     * 打开HDFS文件并返回byte数组，方便Web端下载文件
     *
     * @param path HDFS的相对目录路径，比如：/testDir/b.txt
     */
    public byte[] openWithBytes(String path) {
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = getFileSystem();
            inputStream = fileSystem.open(hdfsPath);
            return IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            log.error("open file", e);
        } finally {
            IOUtils.closeQuietly(inputStream);
            close(fileSystem);
        }
        return null;
    }

    /**
     * 打开HDFS文件并返回String字符串
     *
     * @param path HDFS的相对目录路径，比如：/testDir/b.txt
     */
    public String openWithString(String path) {
        Path hdfsPath = new Path(generateHdfsPath(path));
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = getFileSystem();
            inputStream = fileSystem.open(hdfsPath);
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("open file", e);
        } finally {
            IOUtils.closeQuietly(inputStream);
            close(fileSystem);
        }
        return null;
    }

    /**
     * 打开HDFS上面的文件并转换为Java对象（需要HDFS上门的文件内容为JSON字符串）
     *
     * @param path HDFS的相对目录路径，比如：/testDir/c.txt
     */
    public <T extends Object> T openWithObject(String path, Class<T> clazz) {
        String jsonStr = this.openWithString(path);
        return JSON.parseObject(jsonStr, clazz);
    }
}
