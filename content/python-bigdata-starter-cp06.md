---
title: "Python BigData Starter CP06"
cover: "1.jpg"
category: "python"
date: "2019-02-21"
slug: "python-bigdata-starter-cp06"
tags:
---


​      要处理FTP文件，比如要考虑FTP插件的选择问题。虽然python原生也提供了FTP插件，但是作为一个大数据项目的FTP应用，常常需要处理上百个外部FTP服务器，在这种情况下，就需要考虑选用选择一个功能强大的组件。并进行一些简化的封装处理。





# 6        FTP 文件文件下载

## 6.1     问题背景分析

### 6.1.1用例故事回顾

​        ![6-1 用例故事](/assets/python-bigdata-starter/cp06/6-1 用例故事.png)

​              图 6-1 文件采集用例故事回顾

​        从上图可见，财富银行在武汉市有很多不同的网点，在每个营业网点在会产生一些交易清单，并且每15分钟会合成一个压缩文件放置在FTP接口上。 在中心机房侧，希望在第一时间能够将每个营业网点的文件采集回来，所以在中心机房侧，需要有FTP接口，实现自动的FTP采集功能。

​       在现阶段，为了尽快理解FTP的操作，我们将对用例进行适当的简化处理。



### 6.1.2输入数据处理（I）

​            输入数据为[配置文件URL，server_name，buffer_path]

​      此处的输出有两个对象分别是

1，配置文件URL： 这里给出的是FTP的配置文件的绝对路径，这里配置文件采用.py 的方式进行配置。

2，server_name：是一个list， 给出期望访问的server 名字列表，应用程序将根据这个列表对配置文件进行过滤，提取其中的server进行FTP 处理。

3,buffer_path: 定义一个绝对路径，用于保存采集到的文件。

 假如不提供这个指标，默认的情况下就是根据配置文件来处理。

配置文件的示例为：

### ![6-2 FTP配置信息](/assets/python-bigdata-starter/cp06/6-2 FTP配置信息.png)



​     

### 6.1.3  处理过程（P）

​           下面简要的说明一下相关的处理过程

​    1，用户输出一个输入参数，含配置文件的绝对路径（必选参数），以及期望采集的FTP服务器的列表（可选参数）。

   2，应用程序，引入配置文件，生成待采集服务器的列表。

   3，应用程序提取第一个服务器的配置信息。

   4，应用程序从其中提取一个配置信息，尝试连接FTP服务器，如果无法连接，则返回失败信息。

   5，如果连接成功，就根据配置文件中的path参数，访问FTP路径，遍历FTP路径下的全部文件，形成一个待采集的file list。

6，应用程序根据参数buffer_path， 在对于路径下创建一个文件夹，文件夹的名字为和当前采集的服务器的名字相同，比如server1。

7， 应用程序根据待采集的file list，依次提取文件，并拷贝到对弈的目录下。

8，全部文件拷贝成功以后，就返回一个结果集。信息为

[serve_name, result, number of file, buffer_path ，[[file1,size],[file2,size],…[filen,size]]]

其中server_name ： 用于表达本次采集，采集的服务器的名称。

​     Result： 用于记录本次采集的成果，其中0，成功；1 ，失败。

​     Number of file： 本次采集的文件数量。

​     Buffer path： 本次采集文件存在的路径。

   File list ：本次采集文件的文件列表。每个元素也是一个列表，记录文件名，文件大小。

9，应用程序根据待采集服务器的列表，对后续的服务器完成文件采集。

 10，应用程序返回本次采集的的结果集。



## 6.1.4  处理结果(o)

1,文件拷贝： 将FTP服务器下的目录中的文件，拷贝到本机的路径下。

2，返回结果集。

Result={“server1”: [result, number of file, buffer_path ，[[file1,size],[file2,size],…[filen,size]],

​     “server2”: [result, number of file, buffer_path ，[[file1,size],[file2,size],…[filen,size]] }

​       

## 6.2    FTP的背景知识

   虽然FTP 是一个非常常见的接口协议，也是在大数据应用中广泛应有的协议。 理解FTP的配置参数也不是很困难，不过其中有一个参数涉及到连接模式的问题，在实际的情况下很容易引发配置方面的问题，所以在这里做一个简单的介绍。

   相关的知识参考一下网站。

<http://slacksite.com/other/ftp.html#intro>

### 6.2.1  概述

​          处理防火墙和其他Internet连接问题时最常见的问题之一是主动和被动FTP之间的区别以及如何更好地支持它们中的一个或两个。 希望以下文本有助于澄清如何在防火墙环境中支持FTP的一些困惑。

​      在这里会简要的讲解一下FTP的主动模式，和被动模式的操作命令行，以帮助读者理解FTP协议的工作机制。

​        FTP是一种基于TCP的服务。 FTP没有UDP组件。 FTP是一种不寻常的服务，它使用两个端口，一个'数据'端口和一个'命令'端口（也称为控制端口）。 传统上，这些是端口21用于命令端口，端口20用于数据端口。 然而，当我们发现根据模式，数据端口并不总是在端口20上时；当例外情况出现的时候，使用者就会感觉比较费解。

### 6.2.2 主动FTP

​     在主动模式FTP中，客户端从随机非特权端口（N> 1023）连接到FTP服务器的命令端口，端口21.然后，客户端开始侦听端口N + 1并将FTP命令PORT N + 1发送到FTP 服务器。 然后，服务器将从其本地数据端口（即端口20）连接回客户端的指定数据端口。

​      从服务器端防火墙的角度来看，要支持活动模式FTP，需要打开以下通信通道：

1、FTP服务器的端口21从任何地方（客户端启动连接）

2、FTP服务器的端口21到端口> 1023（服务器响应客户端的控制端口）

3、FTP服务器的端口20到端口> 1023（服务器启动到客户端数据端口的数据连接）

4、FTP服务器的端口20来自端口> 1023（客户端将ACK发送到服务器的数据端口）

   相关的原理见下图所显示：

   ![6-1 主动模式](/assets/python-bigdata-starter/cp06/6-1 主动模式.png)

​              图 6-1 FTP 主动工作模式

​      在步骤1中，客户端的控制端口联系服务器的控制端口并发送操作命令“PORT 1027”。 然后，服务器在步骤2中将ACK发送回客户端的控制端口。在步骤3中，服务器在其本地数据端口上启动连接 客户端在步骤1中。指定的数据端口。 最后，客户端发回ACK，如步骤4所示。

主动模式FTP的主要问题实际上落在客户端。 FTP客户端没有实际连接到服务器的数据端口 - 它只是告诉服务器它正在侦听哪个端口，服务器连接回客户端上的指定端口。 从客户端防火墙，这似乎是一个外部系统启动与内部客户端的连接 – 这种情况通常是被阻止的。



### 6.2.3 被动 FTP

为了解决服务器启动与客户端连接的问题，开发了一种不同的FTP连接机制，这被称为被动模式，或者叫做pasv，也就是在客户端发送PASV命令以后。

在被动模式FTP中，客户端负责启动与服务器的两个连接，解决防火墙过滤从服务器到客户端的传入数据端口连接的问题。 打开FTP连接时，客户端在本地打开两个随机非特权端口（N> 1023和N + 1）。 第一个端口在端口21上与服务器联系，但是并不是发出PORT命令并允许服务器连接回其数据端口，客户端将发出PASV命令。 结果是服务器然后打开一个随机非特权端口（P> 1023）并将P发送回客户端以响应PASV命令。 然后，客户端启动从端口N + 1到服务器端口P的连接以传输数据。

从服务器端防火墙的角度来看，要支持被动模式FTP，需要打开以下通信通道：

1，FTP服务器的端口21从任何地方（客户端启动连接）

2，FTP服务器的端口21到端口> 1023（服务器响应客户端的控制端口）

3，FTP服务器的端口> 1023从任何地方（客户端启动数据连接到服务器指定的随机端口）

4，FTP服务器的端口> 1023到远程端口> 1023（服务器将ACK（和数据）发送到客户端的数据端口）

 相关的原理见下图所显示：

​                                           ![6-2 被动模式](/assets/python-bigdata-starter/cp06/6-2 被动模式.png)

​                                                     6-2 FTP的被动模式

在步骤1中，客户端在命令端口上联系服务器并发出PASV命令。然后，服务器在步骤2中回复客户端，他将准备使用PORT 2024进行数据连接，也就是告诉客户端它正在侦听哪个端口进行数据连接。在步骤3中，客户端然后启动从其数据端口到指定服务器数据端口的数据连接。最后，服务器在步骤4中将ACK发送回客户端的数据端口。

虽然被动模式FTP解决了客户端的许多问题，但它在服务器端打开了一系列问题。最大的问题是需要允许任何远程连接到服务器上的高编号端口。

### 6.2.4  主动模式的问题

​    主动FTP对FTP服务器管理员有利，但对客户端管理员不利。 FTP服务器尝试连接到客户端上的随机高端口，这几乎肯定会被客户端的防火墙阻止。 被动FTP对客户端有利，但对FTP服务器管理员不利。 客户端将同时连接到服务器，但其中一个将连接到随机高端口，这几乎肯定会被服务器端的防火墙阻止。假如客户端采用的是容器化的部署方案，采用主动模式还会带来另外一个更加麻烦的问题。

​      在现实的应用中，大部分的FTP是采用被动模式来进行连接的，假如采用主动模式的话，会从服务器启动连接到客户端。那么这这种情况下，需要从服务器连接到客户端。 假如FTP客户端是部署在一个物理机上，这个一般也没有什么问题，而假如FTP客户端是部署在容器集群中，这个时候就会有问题，因为在容器集群的情况下， FTP客户端的IP地址往往是一个私有地址， 那么这个地址在客户端侧的防火墙强通常是没有办法通过的。至于如何解决，在后续的容器知识中将予以介绍。

## 6.3     FTP 插件-ftputil

​        要处理FTP文件，比如要考虑FTP插件的选择问题。虽然python原生也提供了FTP插件，但是作为一个大数据项目的FTP应用，常常需要处理上百个外部FTP服务器，在这种情况下，就选用选择一个功能强大的组件

  The `ftputil` module is a high-level interface to the [ftplib](https://docs.python.org/library/ftplib.html) module. The [FTPHost objects](https://ftputil.sschwarzer.net/trac/wiki/Documentation#ftphost-objects) generated from it allow many operations similar to those of [os](https://docs.python.org/library/os.html), [os.path](https://docs.python.org/library/os.path.html) and [shutil](https://docs.python.org/library/shutil.html).

​        所以我们考虑选择了ftputil，这个插件实现了对[ftplib](https://docs.python.org/library/ftplib.html) module的高级封装，ftputil设计的思路是通过一个[FTPHost objects](https://ftputil.sschwarzer.net/trac/wiki/Documentation#ftphost-objects) 对象来提供很多操作函数，而这些函数的功能和 [os](https://docs.python.org/library/os.html), [os.path](https://docs.python.org/library/os.path.html) and [shutil](https://docs.python.org/library/shutil.html), 这样熟悉python的你就可以很方便的在FTP应用的上下文中，使用各种文件操作管理。

### 6.3.1 FTPhost

  `FTPHost` instances can be created with the following call:

```
ftp_host = ftputil.FTPHost(server, user, password, account,
                           session_factory=ftplib.FTP)
```

The first four parameters are strings with the same meaning as for the FTP class in the `ftplib` module. Usually the `account` and `session_factory` arguments aren't needed though.

   在 ftputil 的应用中，FTPhost是一个很重要的对象，他可以接受外部FTP的配置文件，主要包含服务器地址，用户名，密码，账号（可选）等信息，以便登录到一个外部的FTP服务器，

当外部FTP连接成功以后，后构建一个FTPhost对象的实例，同时会创建一个关联的FTPsession。

### 6.3.2  Ftputil.session

​        FTPutil.session这个库可以很方便的实现关于主被动模式，以及连接端口的相关信息，以方便适配各种不同的连接模式。

​    The keyword argument `session_factory` may be used to generate FTP connections with other factories than the default `ftplib.FTP`. For example, the standard library of Python 2.7 contains a class `ftplib.FTP_TLS` which extends `ftplib.FTP` to use an encrypted connection.

In fact, all positional and keyword arguments other than `session_factory` are passed to the factory to generate a new background session. This also happens for every remote file that is opened; see below.

This functionality of the constructor also allows to wrap `ftplib.FTP` objects to do something that wouldn't be possible with the `ftplib.FTP` constructor alone.

​        在前面的章节介绍过，现实世界的FTP服务器的连接方式多种多样，为了适配不同的应用场景，我们可以定制化一下专有的连接模式。 在大规模的FTP服务器连接中，这种配置能力是很重要的。

```
def session_factory(base_class=ftplib.FTP, port=21, use_passive_mode=None,
                    encrypt_data_channel=True, debug_level=None):
Create and return a session factory according to the keyword
    arguments.

    base_class: Base class to use for the session class (e. g.
    `ftplib.FTP_TLS` or `M2Crypto.ftpslib.FTP_TLS`, default is
    `ftplib.FTP`).

    port: Port number (integer) for the command channel (default 21).
    If you don't know what "command channel" means, use the default or
    use what the provider gave you as "the FTP port".

    use_passive_mode: If `True`, explicitly use passive mode. If
    `False`, explicitly don't use passive mode. If `None` (default),
    let the `base_class` decide whether it wants to use active or
    passive mode.

    encrypt_data_channel: If `True` (the default), call the `prot_p`
    method of the base class if it has the method. If `False` or
    `None` (`None` is the default), don't call the method.

    debug_level: Debug level (integer) to be set on a session
    instance. The default is `None`, meaning no debugging output.

    This function should work for the base classes `ftplib.FTP`,
    `ftplib.FTP_TLS` and `M2Crypto.ftpslib.FTP_TLS` with TLS security.
    Other base classes should work if they use the same API as
    `ftplib.FTP`.
```

### 6.3.3  listdir(path)

​      在对象FTPhost提供了很多的方法进行文件的管理，其中有一个很重要的功能是对文件进行遍历。相关的函数简要说明如下：

  [Retrieving information about directories, files and links](https://ftputil.sschwarzer.net/trac/wiki/Documentation#id20)

·         `listdir(path)`

returns a list containing the names of the files and directories in the given path, similar to [os.listdir](https://docs.python.org/library/os.html#os.listdir). The special names `.` and `..` are not in the list.

`FTPHost` objects contain an attribute named `path`, similar to [os.path](https://docs.python.org/library/os.path.html). The following methods can be applied to the remote host with the same semantics as for `os.path`:

```
abspath(path)
basename(path)
commonprefix(path_list)
dirname(path)
exists(path)
getmtime(path)
getsize(path)
isabs(path)
isdir(path)
isfile(path)
islink(path)
join(path1, path2, ...)
normcase(path)
normpath(path)
split(path)
splitdrive(path)
splitext(path)
walk(path, func, arg)
```

Like Python's counterparts under [os.path](https://docs.python.org/library/os.path.html), `ftputil`'s is... methods return `False` if they can't find the path given by their argument.



## 6.4     class ftpconn()

​       有了一个高级的FTP组件，ftputil， 我们可以进一步实现封装为一个ftpconn()类，利用这个类，可以很方便的实现远程服务器的连接访问。

​    ![6-3 key object of FTP handling](/assets/python-bigdata-starter/cp06/6-3 key object of FTP handling.png)

​                     图 6-3 ftpconn() 对象

### 6.4.1  函数listdir（）

​        当我们连接到一个外部fTP服务器的时候，首先要考虑能够收集到路径下文件的名字，由于在一个目录下可能有文件，也可能有文件夹。因为不能预先知道一个FTP目录下，目录的层数， 所以这里再次需要依赖递归编程的思想。

​        采用一个队列，利用循环处理，实现递归编程的目标。

相关的设计见下图。

​             ![6-4 function listdir](/assets/python-bigdata-starter/cp06/6-4 function listdir.jpg)

​         图 6-4  基于递归识别的文件遍历

​         从上图可见，相关的方案是设定两个list，分别是

1，filelist[]:  是一个记录文件名称（绝对路径）的列表，用于记录识别的文件的绝对路径名称， 也就是文件的URL，通过这个URL可以识别到一个特定的文件。

2，dirlist[]: 是一个记录访问路径的列表，初始的元素通过传参的方式获得，后续通过不断的路径扫描，识别到的文件目录来增加记录。

相关文件遍历的思路，简要说明如下：

1，初始化一个filelist[] 列表，用于保留识别的文件名字。

2，初始化1个dirlist = [path]， 其中的元素是路径。

3， 针对dirlist进行循环遍历，利用dirlist.pop()操作，从队列中提取一个目录元素（同时删除），利用函数 names = self.ftphost.listdir(absdir)，获取相关路径下的所有文件和文件夹信息，并传入到列表names中。

4， 针对列表names 进行判断，如果是一个文件夹，将压入到列表dirlist = [path]下，如果是一个文件，则提取绝对路径，将压入到filelist[]下。

 经过递归的处理，就能遍历全部的文件。

​          ![6-6 queue+list](/assets/python-bigdata-starter/cp06/6-6 queue+list.jpg)

​                                       图 6-6   利用list 实现queue的功能

​       从上图可见，这里利用list() 来实现了queue的功能。说明我们说python是一种面向list的编程语言，对list的功能进行了大量的完善。

​     

### 6.4.2     download(self,remote,local)

而利用对象 FTPhost的内置方法，可以很容易的实现相关的下载操作。

### 6.4.3  封装的目的

ftpUtil本身已经提供了非常多的函数和方法， 然而假如直接使用ftputil的话， 相关的代码比较难以维护，也很难理解。所以我们考虑采用封装ftputil 组件，形成我们自己的组件。

以def listdir(self, path) 函数为例，ftputil也提供一个同名函数，而在ftpconn()中，也提供这个函数，同时从应用的角度，处理起来更加的简单。

### 6.5     设计模型

   ![6-8 class vs instance](/assets/python-bigdata-starter/cp06/6-8 class vs instance.png)

​                         图 6-7  设计模型



​     从上图可见，getter（）函数可以方便的完成最后的文件下载的功能。而getter（）函数主要有三部分组成

1，ftp：核心的FTP处理对象，他是通过class ftpconn()实例化而来，所以也包含了全部的功能。主要用的功能包含：

1）listdir：遍历路径

2）download：从远程下载文件到本地



2、to_local(file name): 将远程的文件名，转换为本地的文件名。因为用例模型中，有一个要求：buffer_path: 定义一个绝对路径，用于保存采集到的文件。

所以需要实现一个文件名的转换处理。



3，result []: 记录相关的处理集合。



1.5.1 如何获取远端FTP服务器下的文件列表

![6-9 如何扫描文件](/assets/python-bigdata-starter/cp06/6-9 如何扫描文件.png)

​        filelist = ftp.listdir(ftpinfo['path'])

从上面的代码，我们可以看到，利用前面封装的ftpconn() ,我们可以很容易的构建一个对象，来实现ftp的一些功能。由于存在打印的要求，所以需要做一些特殊的处理。

1，创建一个列表result[]，用于保留相关的结果集。

2，遍历配置文件conn_info,确定所有需要连接的外部FTP服务器的信息。

3，初始化一个结果元素，而这个结果元素也是1个list，分别包含以下的元素：

3.1 servername： 本次连接的服务器的名字。

3.2  1： 本次连接操作的结果，由于事先并不清楚外部的FTP服务器能不能连接成功，所以设定为“1”，失败，如果后续连接成功，则更新为“0”。

3.3 下载文件次数 0： 在初始的情况下，设定为下载为0个文件

3.4  下载文件的详细信息[]: 用来后续记录详细的文件信息。

4，尝试将对象ftpcpnn()进行实例化，创建一个对象FTP， 其入参是FTP服务器的配置信息；而返回数值是一个FTPhost对象，当外部FTP连接成功以后，后构建一个FTPhost对象的实例，同时会创建一个关联的FTPsession。

5, 在列表result[]中追加一条记录，用于记录当前FTP server的访问结果。

6，将本次FTP连接操作设定为成功。

7，调用[ftp.listdir](ftp://ftp.listdir)（）方法，获得目标路径下的文件列表。





### 6.5.2 如何下载文件

![6-10 如何下载文件](/assets/python-bigdata-starter/cp06/6-10 如何下载文件.png)

​      根据上面的代码，我们可以发现下载的方案如下：

1， 根据前面环节获得的filelist，得到相关的下载文件清单。

2，将远程的文件名转换为本地的文件名（添加相关的前缀信息）。

3，由于文件下载可能成功，也可能失败。 所以使用try 操作。

4，调用[ftp.download()](ftp://ftp.download())方法，下载文件。源文件是远端的文件，目标文件是本地的文件。

5，将结果集中，下载文件数 添加1。

6,将结果集中的第四个元素， 文件信息列表中，追加1条记录， 这个记录中包含两个元素，[文件名称，文件大小]。

7,在结果集list中追加1条记录。 记录当前FTP服务器下载操作的结果。



### 5.6     总结

​        在本次的项目中，我们尝试如何进行FTP下载， 我们重点巩固了三方面的知识。

1，组件集成和面向对象的设计, 通过构建了一个ftpconn（）的对象，使得我们可以很方便的扫描1个远程的FTP服务器，并从中下载需要的文件。 虽然这个是非常底层的操作，但是利用ftpconn（）的对象，我们仅仅需要理解接口的调用方式，就可以获得令人满意的结果。

2，本方案中再次演示如何利用list来实现递归编程的方法。由于python的list提供了强大的list.pop(), list.append()方法，所以我们可以通过list来实现queue的功能。 而相关的功能，为递归调用提供了一种轻量化的堆栈服务。

3，由于连接外部的FTP服务服务器，可能成功，也可能失败，所以利用列表list, 设计了大量的日志功能，监控ftp操作的结果，再次验证了python是一种面向列表的编程语言。
