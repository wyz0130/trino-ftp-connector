package org.ebyhr.trino.storage.utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.ebyhr.trino.storage.StorageConfig;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

public class FtpUtils
{


    public static FTPClient getFTPClient(StorageConfig storageConfig)
    {
        FTPClient ftpClient = null;
        try {
            ftpClient = new FTPClient();
            ftpClient.setConnectTimeout(60000);
            if (storageConfig.getFtpHost() != null) {
                ftpClient.connect(storageConfig.getFtpHost(), Integer.valueOf(storageConfig.getFtpPort()));// 连接FTP服务器
            }
            else {
                ftpClient.connect(storageConfig.getFtpHost());// 连接FTP服务器
            }
            if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
                if (ftpClient.login(storageConfig.getFtpUser(), storageConfig.getFtpPassWord())) {// 登陆FTP服务器
                    if (FTPReply.isPositiveCompletion(ftpClient.sendCommand("OPTS UTF8", "ON"))) {// 开启服务器对UTF-8
                        // 的支持，如果服务器支持就用UTF-8编码，否则就使用本地编码（GBK）.
                        ftpClient.setControlEncoding("UTF-8");
                    }
                    else {
                        ftpClient.setControlEncoding("GBK");
                    }
                    ftpClient.enterLocalPassiveMode();// 设置被动模式
                    ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置传输的模式，以二进制流的方式读取
                    ftpClient.enterLocalPassiveMode();
                    System.out.println("FTP服务连接成功！");
                }
                else {
                    System.out.println("FTP服务用户名或密码错误！");
                    disConnection(ftpClient);
                }
            }
            else {
                System.out.println("连接到FTP服务失败！");
                disConnection(ftpClient);
            }
        }
        catch (SocketException e) {
            e.printStackTrace();
            disConnection(ftpClient);
            System.out.println("FTP的IP地址可能错误，请正确配置。");
        }
        catch (IOException e) {
            e.printStackTrace();
            disConnection(ftpClient);
            System.out.println("FTP的端口错误,请正确配置。");
        }
        return ftpClient;
    }

    /**
     * 关闭FTP服务链接
     *
     * @throws IOException
     */
    public static void disConnection(FTPClient ftpClient)
    {
        try {
            if (ftpClient.isConnected()) {
                ftpClient.disconnect();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取文件夹下的所有文件信息
     *
     * @param path 文件路径
     */
    public static FTPFile[] getFTPDirectoryFiles(FTPClient ftpClient, String path)
    {
        FTPFile[] files = null;
        try {
            ftpClient.changeWorkingDirectory(path);
            files = ftpClient.listFiles();
        }
        catch (Exception e) {
            e.printStackTrace();
            //关闭连接
            disConnection(ftpClient);
            System.out.println("FTP读取数据异常！");
        }
        return files;
    }


    /**
     * 获取文件夹下的所有文件信息
     *
     * @param path 文件路径
     */
    public static InputStream getFTPFile(FTPClient ftpClient, String path, String fileName)
    {
        InputStream in = null;
        try {
            ftpClient.changeWorkingDirectory(path);
            FTPFile[] files = ftpClient.listFiles();
            if (files.length > 0) {
                in = ftpClient.retrieveFileStream(fileName);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("FTP读取数据异常！");
        }
        finally {
            //关闭连接
            disConnection(ftpClient);
        }
        return in;
    }

//    public static void main(String args[])
//    {
//        InputStream in = null;
//        BufferedReader br = null;
//        try {
//            String path = "/root/var/ftp/test";
//            //读取单个文件
//            FTPClient ftpClient = getFTPClient(ftpHost, ftpPort, ftpUser, ftpPassWord);
//            String fileName = "1.csv";
//            in = getFTPFile(ftpClient, path, fileName);
//            if (in != null) {
//                br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
//                String data = null;
//                while ((data = br.readLine()) != null) {
//                    String[] empData = data.split(",");
//                    System.out.println(data);
//                }
//            }
//
//            //读取文件夹下的所有文件
//            FTPClient ftpClient1 = getFTPClient(ftpHost, ftpPort, ftpUser, ftpPassWord);
//            FTPFile[] files = getFTPDirectoryFiles(ftpClient1, path);
//
//            if (files != null && files.length > 0) {
//                for (int i = 0; i < files.length; i++) {
//                    if (files[i].isFile()) {
//                        in = ftpClient1.retrieveFileStream(files[i].getName());
//                        br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
//                        String data = null;
//                        while ((data = br.readLine()) != null) {
//                            System.out.println(data);
//                        }
//                        if (br != null) br.close();
//                        if (in != null) in.close();
//                    }
//
//                }
//            }
//            //关闭连接
////            disConnection(ftpClient1);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally {
//            try {
//                //关闭流
//                if (br != null) br.close();
//                if (in != null) in.close();
//            }
//            catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

}
