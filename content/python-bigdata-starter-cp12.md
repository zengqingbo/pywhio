---
title: "Python BigData Starter CP12"
cover: "4.jpg"
category: "python"
date: "2019-04-07"
slug: "python-bigdata-starter-cp12"
tags:
---​

​       分布式处理是大数据处理的一个核心基础，所以我们在本章节将介绍这样一个案例，作为python 大数据入门的最后一个练习。



## 12  远程文件解压

​     在前大数据时代，通常1台计算机，或者1台unix服务器就能够搞定巨大部分的信息化项目，所以项目的大部分部组件可以安装到1台服务器上，组件只需要在服务器内进行协同；但是到了大数据时代，因为数据的规模比较大，所以实施一个项目，往往需要多台机器实施项目，这就意味着项目的组件必须分布到不同的服务器上，组件之间必须跨服务器进行协同工作。虽然做事情的逻辑是一样的，然而跨机器处理的难度，远远大于在同一台机器的处理难度。虽然从单机处理，到多机处理， 从程序处理的角度是迈出了一小步，而从大数据的角度来看，则是迈出了关键的第一步。

## 12.1   问题背景介绍

​       用例故事描述

![12-1 远程文件加压用例故事]((/assets/12-1 远程文件加压用例故事.png)

​                   图 12-1 远程文件解压服务

  从上图可见，我们假设整个应用部署在两台服务器上，其中：

1，          服务器1：上部署了FTP文件采集服务，采集到的压缩格式的文件放置在zip file warehouse下，解压缩以后的文件放置在unzip file warehouse下。

2，          服务器2：提供了解压缩服务，他可以接受压缩文件，返回解压缩的文件，以及解压操作的结果。

### 12.1.1 输入数据处理（I）

   输入数据包含以下的三个要素 [zipfile path,  unzipfile path,  if_unzip.cfg]，下面将相关的要素进行说明：

1、          zipfile path：用于保存原始压缩文件的绝对路径。

2、          unzipfile path：用于保存完成解压文件的绝对路径。

   3， if_unzip.cfg：用于配置提供远程解压服务的服务器地址，和相关的配置信息。

​      本实验阶段，先考虑使用1台远程解压服务器。希望能够兼容多台远程解压服务器的模式。



### 12.1.2 处理过程（P）

1，          应用程序读取zipfile path，扫描所有需要解压的文件（暂时不考虑下层文件夹的问题），建立待解压的文件列表。

2，          应用程序提取1个压缩格式的文件，调用本地文件解压接口，将压缩文件传递到远程的server2.

3，          Server2 接收到压缩文件以后，对文件进行递归解压操作，生成解压文件流，并生成解压缩格式的文件。文件暂存在本地的临时文件夹中。

4，          全部文件解压完毕以后，server 侧生成发送列表：{filename1:file object1, filename2:file object2, filename n,fileobject n}

5，          Server2 生成解压缩结果，相关的结果为dict形式，

{layer1_file.zip:[ 解压结果，number of next layer zipfile，number of next layer unzipfile  ], layer2_file1.zip: [ 解压结果，number of next layer zipfile，number of next layer unzipfile ]}。

**备注：**number of next layer zipfile，如果能够解压成功，可以判断下一层的压缩文件数量，非压缩文件的数量。解压结果取值0（成功），1（失败）。

1、 Server2回调相关的本地接口，将server 侧的完成解压文件回   传给到server1的服务器。本地接口将文件写入unzipfile path。

2、 Server2回调相关的本地接口，将server 侧的完成结果回传

3、server1 根据server2的处理结果，生成当前zip文件的解压结果。

4、server1 继续处理其他的文件，直到全部的文件加压缩完毕。

### 12.1.3输出结果（O）

​     输出结果包含两个部分

1，文件解压概述：

​     [执行结果，解压缩文件总数，成功处理子文件数量] ]

其中执行结果： 0  成功， 1 失败 ，2 部分失败

子文件数量：3

成功处理子文件的数量：3



2、单个文件解压详情：

{file1.zip:[解压结果，remote server1, starttime,endtime, number of failed unzip file, number of unzip file received],

File2.zip:[解压结果，remote server1, starttime,endtime, number of failed unzip file, number of unzip file received]}

**备注： starttime** **：记录开始调用接口的时间。**

​       **Endtime****：接收到完成解压的文件，以及解压结果以后的时间。**



## 12.2     项目环境准备

​       在我们训练的第一本书《python 大数据入门》系列的培训案例中，基本上都是在1台电脑上就能够完成，即使是FTP的实验也可以利用一些变通的方法来实现，而这个分布式实验，却是必须要在两台服务器上来实现。所以，在本实验中，要简要介绍一下环境的准备工作。

### 12.2.1 计算环境准备

![1- 硬件准备环境](/assets/1- 硬件准备环境.png)

​                  12-2 两台服务器的计算环境

   从上图可见，用于验证本项目的硬件需要两台计算机。

1，server 计算机：用于提供解压缩的服务。机器的主机名是eagle。

2，client计算机：用于接受用户发起的解压缩的服务请求。机器的主机名是tiger。

以上的环境，可以用真实的两台电脑，也可以使用其他的虚拟机的方案。在本方案中，我们计划采用虚拟机的方案，其中使用的操作系统是：centos 7.5



### 12.2.2 Python3 环境准备

在本系列的练习案例中，全部采用python3作为程序语言编程环境。然而在centos下，默认的python环境为python 2.7 ，在这种情况下，需要单独安装python3.7。

安装方法，可以参考以下的文档。

<https://danieleriksson.net/2017/02/08/how-to-install-latest-python-on-centos/>

### 12.2.3 服务端的包安装

​     由于本项目需要使用一个特殊的包叫做socketserver。  这个socketserver需要进行额外的安装，安装的过程需要联网。

​      socketserver is not included in official Python package.  Internet is needed.

​    相关的安装命令如下：

​    \#pip3.7 install Werkzeug --upgrade



### 12.2.4 为多网卡服务器配置监听端口

当我们配置好服务器以后，如果要服务器能够工作，就需要配置一个监听端口。在server端的python代码中做如下的配置：

​    \#HOST, PORT = "192.168.153.129", 9999

​    HOST, PORT = "", 9999





[root@eagle cp12]# netstat -na | grep 9999

tcp   0   0 0.0.0.0:9999      0.0.0.0:*      LISTEN



![2-配置server端的监听端口](/2-配置server端的监听端口.png)

​              12-3  server 侧的TCP端口监听方案

在现实的大数据处理环节中，很多主机往往需要配置多网卡，所以在多网卡环境（multi-home server）环境下，需要认真检查具体的监听路径。 这里有两种配置方案：

1，HOST, PORT = "192.168.153.129", 9999

在这种方案下，主机仅仅监控来自本机网卡"192.168.153.129"上的9999端口。

2，HOST, PORT = "", 9999

  在这种方案下，主机会监控来自本机所有网卡的9999端口。

如果要确认这种配置，就可以使用一下的MML：

\# netstat -na | grep 9999



### 12.2.5 为客户端设置服务器地址

   在客户端程序中，要设置服务器地址，相关的配置文件为：

​     if_unzip_cfg = ('eagle', 9999)



  在本方案中，因为是虚拟机，所以是设定的主机名，也可以使用ip地址的方式。

## 1.3     设计模型

### 1.3.1 对象结构概述

   ![3-应用部署的包结构](/assets/3-应用部署的包结构.png)

​                  12-4  分布式组件模型说明

从上图可见，相关的对象结构，分为2部分，主要见上图所见。

其中，主要的对象作用，简要说明如下：

1，client.py:  负责接收客户的解压调用请求。

2，serializer.py：负责将客户提交的压缩文件，进行序列化处理，转为网络传输。这个对象，服务器和客户端均需要。

3，server.py:  负责监听网络的请求，接收socket流。 并执行后续的操作。

4，decompress.py: 负责执行具体的解压缩处理。



### 12.3.2 序列化与反序列化

  在分布式编程中，序列化和反序列化是非常重要的功能，所以我们稍微解释一下。

根据官网的解释：

The [pickle](https://docs.python.org/3/library/pickle.html#module-pickle) module implements binary protocols for serializing and de-serializing a Python object structure.*“Pickling”* is the process whereby a Python object hierarchy is converted into a byte stream, and *“unpickling”* is the inverse operation, whereby a byte stream (from a [binary file](https://docs.python.org/3/glossary.html#term-binary-file) or [bytes-like object](https://docs.python.org/3/glossary.html#term-bytes-like-object)) is converted back into an object hierarchy. Pickling (and unpickling) is alternatively known as “serialization”, “marshalling,” [[1\]](https://docs.python.org/3/library/pickle.html#id6) or “flattening”; however, to avoid confusion, the terms used here are “pickling” and “unpickling”.。



 pickle模块实现了用于序列化和反序列化Python对象结构的二进制协议。“Pickling”的意涵是将Python对象层次结构转换为字节流的过程，“unpickling”是反向的操作，即字节流（来自 二进制文件或类似字节的对象）被转换回对象层次结构。 Pickling（和unpickling）也可称为“序列化”，“编组（marshalling）”，或“扁平化（flattening）”; 但是，为了避免混淆，这里使用的术语是“Pickling”和“unpickling”。

pickle提供了一个简单的持久化功能。可以将对象以文件的形式存放在磁盘上。pickle模块只能在[**Python**](http://lib.csdn.net/base/python)中使用，python中几乎所有的数据类型（列表，字典，集合，类等）都可以用pickle来序列化，

这样就可以非常方便的进行分布式运算。



### 12.3.3 SocketServer

socket并不能多并发，只能支持一个用户，socketserver 简化了编写网络服务程序的任务，socketserver是socket的在封装。socketserver在python2中为SocketServer,在python3种取消了首字母大写，改名为socketserver。socketserver中包含了两种类，一种为服务类（server class），一种为请求处理类（request handle class）。前者提供了许多方法：像绑定，监听，运行…… （也就是建立连接的过程） 后者则专注于如何处理用户所发送的数据（也就是事务逻辑）。一般情况下，所有的服务，都是先建立连接，也就是建立一个服务类的实例，然后开始处理用户请求，也就是建立一个请求处理类的实例。

![4-five class of socketserver](/assets/4-five class of socketserver.png)

​                              12-5 socket server 的基类



利用socket server 可以极大的简化服务器端的编程方案，其中包含了5大核心组件。

 通过使用以上5大核心组件，可以方便的进行服务器的编程工作。



## 12.4     客户端软件

  在这个原型中，客户端的处理是比较复杂的，他要负责能够处理和用户的交换操作，接收输入的参数，调用远程方法，并向客户输出返回值。所以这一部分的设计，会比较复杂，下面首先阐述一下相关的的对象模型。

   ![5-object model of client](/assets/5-object model of client.png)

​                  12-6 客户端软件的对象模型

从上图可见，相关的client端的软件，主要是由四部分的函数对象组成的。



1,**input argument**： 输入参数list，用于接收客户从客户端提交的和解压缩操作相关的参数。

2，decompress_fun: 解码函数，负责扫描期待解压的路径，生成待解压的压缩文件列表，并调用远程解压函数。

3，remote_decompress: 负责调用远程的解压缩服务，并等待接收远程服务器回传的结果。

4，result{}：负责反馈解压缩的结果集。



  下面，重点针对解压函数进行讲解。

### 12.4.1 decompress_fun()

   在客户端的软件中对象中，decompress_fun()主要负责负责扫描期待解压的路径，生成待解压的压缩文件列表，并调用远程解压函数等功能，主要的处理过程说明如下：

1、接收用户输入的传参，并在屏幕上打印传参信息。

```
print(f'{Fore.GREEN} zipfile_path: %s \n unzipfile_path: %s \n if_unzip_cfg: %s {Style.RESET_ALL}' % (zipfile_path, unzipfile_path, if_unzip_cfg))
```



2、扫描zipfile_path路径下的所有文件，形成一个filelist[] 列表，记录当前路径下的文件集合。

3、针对filelist[]中的文件进行检查，筛选出其中的压缩格式的文件， 并形成一个新的zipfilelist[] 列表，激励了当前路径下所有压缩格式的文件集合。 也就是待解压的文件集合。

4、创建一个解压结果集合的字典 result{}。

5、针对zipfilelist[]中的压缩文件，逐个进行处理，调用remote_decompress()函数进行处理，调用时候的传入的参数包括：

(zipfile_path, unzipfile_path, if_unzip_cfg)

其中if_unzip_cfg中配置了远程解压缩服务的地址。

6，在调用的过程中，会生成本次解压缩的结果信息。

7，待所有的待解压文件处理完毕以后，反馈相关的结果集合。



### 12.4.2 远程调用传参

​     在本案例中，需要调用一个远程接口进行加压缩操作，下面我们看看传参的方案：

```
successnum = remote_decompress(zipfilename, os.path.join(unzipfile_path, os.path.splitext(os.path.basename(zipfilename))[0]), *if_unzip_cfg)
```

从上面的表达方式可见，依次传递了四个参数，分别是

1，zipfilename：待解压的压缩文件。

2、os.path.join(unzipfile_path, os.path.splitext(os.path.basename(zipfilename))[0])

   解压以后的位置

3，提供解压服务的主机，来自配置if_unzip_cfg。

4、提供解压服务的主机端口，来自if_unzip_cfg。

​    在此处解释一下，相关的参数传递的过程，如何将1个元组，转换为几个单一的元素。

![6-传参的方案](/assets/6-传参的方案.jpg)

​                                   12-7 python 3 传参说明

在传递参数的时候，列表或元组前面的“*‘， 会把元组或列表变成一个个的单一的元素进行传递。

在这里 if_unzip_cfg = ('localhost', 9999) 是元组

而在调用的时候，使用一下的表达方式：*if_unzip_cfg

就转换为2个元素，

def remote_decompress(zipfilename, distdir, host , port = 9999)



相关的传参方法的解释参考一下的链接：

<https://www.python-course.eu/python3_passing_arguments.php>

### 12.4.3 Python socket 编程

​    socket通常也称作"套接字"，用于描述IP地址和端口，是一个通信链的句柄，应用程序通常通过"套接字"向网络发出请求或者应答网络请求。

socket起源于Unix，而Unix/Linux基本哲学之一就是“一切皆文件”，对于文件用【打开】【读写】【关闭】模式来操作。socket就是该模式的一个实现，socket即是一种特殊的文件，一些socket函数就是对其进行的操作（读/写IO、打开、关闭）

socket和file的区别：

·         file模块是针对某个指定文件进行【打开】【读写】【关闭】

·         socket模块是针对 服务器端 和 客户端Socket 进行【打开】【读写】【关闭】

相关的原理见下图所显示：

   ![7-socket 通信的原理](/assets/7-socket 通信的原理.png)

​                   12-8 socket 通信的原型

主要的语法解释如下：

**sk = socket.socket(socket.AF_INET,socket.SOCK_STREAM,0)**

参数一：地址簇

　　socket.AF_INET IPv4（默认）

　　socket.AF_INET6 IPv6

　　socket.AF_UNIX 只能够用于单一的Unix系统进程间通信



参数二：类型



　　socket.SOCK_STREAM　　流式socket , for TCP （默认）

　　socket.SOCK_DGRAM　　 数据报式socket , for UDP



　　socket.SOCK_RAW 原始套接字，普通的套接字无法处理ICMP、IGMP等网络报文，而SOCK_RAW可以；其次，SOCK_RAW也可以处理特殊的IPv4报文；此外，利用原始套接字，可以通过IP_HDRINCL套接字选项由用户构造IP头。

　　socket.SOCK_RDM 是一种可靠的UDP形式，即保证交付数据报但不保证顺序。SOCK_RAM用来提供对原始协议的低级访问，在需要执行某些特殊操作时使用，如发送ICMP报文。SOCK_RAM通常仅限于高级用户或管理员运行的程序使用。

　　socket.SOCK_SEQPACKET 可靠的连续数据包服务



参数三：协议

　（默认）与特定的地址家族相关的协议,如果是 0 ，则系统就会根据地址格式和套接类别,自动选择一个合适的协议

**1、sk.connect(address)**

```
　连接到address处的套接字。一般，address的格式为元组（hostname,port）,如果连接出错，返回socket.error错误。
```



**2、sk.sendall(string[,flag])**

```
将string中的数据发送到连接的套接字，但在返回之前会尝试发送所有数据。成功返回None，失败则抛出异常。
```



**3、sk.recv(bufsize[,flag])**

```
接受套接字的数据。数据以字符串形式返回，bufsize指定最多可以接收的数量。flag提供有关消息的其他信息，通常可以忽略。
```



以上内容，参加一下链接

 <https://www.cnblogs.com/fanweibin/p/5053328.html>

开启相关的操作以后，主要的操作报告：





### 12.4.4  remote_decompress（）

   经过传参以后，将调用函数 remote_decompress()来进行处理。总体的对象模型说明如下：

 ![8-基于socket 的加压缩方案](/assets/8-基于socket 的加压缩方案.png)

​                  12-9 远程解压接口的对象模型

​       

​      从上图可见，利用socket提供的强大的方法，可以非常方便的实现socket的远程方法调用。实现了流操作的方法。

## 12.5     服务器端的软件

### 12.5.1 Socket server原理简介

SocketServer内部使用 IO多路复用 以及 “多线程” 和 “多进程” ，从而实现并发处理多个客户端请求的Socket服务端。即：每个客户端请求连接到服务器时，Socket服务端都会在服务器是创建一个“线程”或者“进 程” 专门负责处理当前客户端的所有请求。

注：导入模块的时候 3.x版本是socketserver 2.x版本是SocketServer

![9-server 端的原理](/assets/9-server 端的原理.png)

​                                             12-10 TCP server 的应用模型

1.ThreadingTCPServer

ThreadingTCPServer实现的Soket服务器内部会为每个client创建一个 “线程”，该线程用来和客户端进行交互。

1.1 ThreadingTCPServer基础

使用ThreadingTCPServer:

创建一个继承自 SocketServer.BaseRequestHandler 的类
 类中必须定义一个名称为 handle 的方法
 启动ThreadingTCPServer





参考一下的链接：

<https://www.cnblogs.com/zhangkui/p/5655428.html>



### 12.5.2 服务端软件

 ![10-server 端 software](/assets/10-server 端 software.png)

​         12-11 服务端软件的模型

​                  

  上图是服务器端软件的对象模型，整个对象模型比较简单。其中核心是初始化了一个对象 MyTCPHandler  ,他负责处理具体的socket 请求。

其中有三个核心组成部分，分布是

1，**self.request** : 他代表了一个和客户端连接的socket；

self.request is the TCP socket connected to the client

他提供了socket 发送，socket接收的方法。



2，**self.data**： 用于接收从客户端传递过来的流对象。



3，**decompress()**: 具体实现解压缩处理的函数。



### 12.5.3 应用效果

1，本地调试结果

![CP12应用](/assets/CP12应用.png)



2， 分布式方案下客户端观察

2.1 客户端的调用显示

![client result](/assets/client result.jpg)

2.2  解压缩的结果

![client-result](/assets/client-result.jpg)



2.3 服务端的调用显示

![server result](/assets/server result.jpg)
