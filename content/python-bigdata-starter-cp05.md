---
title: "Python BigData Starter CP05"
cover: "5.jpg"
category: "python"
date: "2019-02-18"
slug: "python-bigdata-starter-cp05"
tags:
---
​
        在压缩文件和解压缩文件是一种非常基础的流操作模式，而python提供了强大的接口，可以非常方便的处理这些文件压缩和解压缩操作。



# 5 多文件压缩与解压

在压缩文件和解压缩文件是一种非常基础的流操作模式，而在大数据的应用场景中，由于经典的Volume特征，数据的size会非常大，所以从节省存储设备的角度，时常要面对压缩文件和解压缩文件的任务，所以如何高效的处理压缩文件和解压缩文件也是非常重要的一个技能。





## 5.1 问题背景分析

### 5.1.1用例故事回顾

​    ![5-1 用例故事](/assets/python-bigdata-starter/cp05/5-1 用例故事.jpg)

​     图 5-1  文件压缩的用例故事

​          结合上图的用例故事，我们可以将问题场景做一次细化：

1， 待处理的问题是n 个 xml格式的文件。

2，处理过程分为两步处理法。第一步是将每个xml文件压缩为zip格式的压缩文件。

第二步是将多个压缩文件二次压缩为一个聚合压缩文件。二次压缩文件为zip格式。

3， 将二层压缩文件拷贝到sender目录。

4，将两层压缩文件从sender目录拷贝到receiver目录。

5，将二层压缩文件进行解压，首先识别出二层压缩文件中所有的子压缩文件，顺序针对每一个子压缩文件，转换为xml文件，并放置到destination目录下。

用例执行完毕。



### 5.1.2输入数据分析（I）

从应用的角度，输出数据包含三个要素，分别是

1， 程序处理关联的文件路径 [source, sender,receiver,destination];

2,   期待压缩的文件类型，在本案例中，选择为xml ，也就是说在source 目录下的所有xml格式的文件都纳入压缩处理。

3， 期待压缩的二层文件命名，例子 xml2layeragg.zip



### 5.1.3 处理过程（P）

整个的处理过程简要说明如下

1，应用程序扫描source目录下的所有文件，将xml格式的文件识别出来，将文件名传入一个zip file list当中。

2，应用程序在sender目录打开一个xml2layeragg.zip格式的文件，获得其中的句柄。

3，应用程序遍历file list，选取第一个文件，转换为压缩格式，并写入到xml2layeragg.zip的句柄中，顺序选取后续的文件转换为压缩格式，追加到xml2layeragg.zip的句柄中。当最后的文件处理完毕，关闭xml2layeragg.zip.

\4. 应用程序给出本次压缩操作的返回值，返回值包括

执行结果： 0  成功， 1 失败

子文件的数量：3

二层压缩文件的大小：size

5， 应用程序将xml2layeragg.zip拷贝到sender路径下。

6，应用程序返回 本次执行的结果：0 成功，1 失败。

7，应用程序将xml2layeragg.zip拷贝到receiver 路径下。

8，应用程序返回本次执行的结果：0 成功，1失败

9 ，应用程序将xml2layeragg.zip对应的压缩格式的文件流进行识别，转换为在内存中的中间流， 并遍历其中包含的子文件。将子文件的文件名传入一个 unzip file list中。

10，应用程序从unzip file list中提取第一个子文件，compressed file 1，加载在内存中。

11, 应用程序，在destination路径下，打开一个文件的句柄，准备写入文件，文件名和compressed file 1同名，但是扩展名为xml。

12 ，应用程序将第一个子文件进行解压，并写入到 file1.xml中，处理完毕以后，关闭第一个压缩文件。

13 应用程序顺序处理后续的文件，直到把所有的文件处理完毕。

14，应用程序返回本次执行的结果。

执行结果： 0  成功， 1 失败 ，2 部分失败

子文件数量：3

成功处理子文件的数量：3

### 5.1.4 处理结果（O）

应用程序，返回本次的执行结果。结果是一个list的形式。

[[执行结果，压缩子文件数量，size]，[执行结果]，[执行结果]，[执行结果，解压缩文件总数，成功处理子文件数量] ]



### 5.1.5入参与返回数值

   在本项目中，再次归纳相关的入参和返回值：

  1，入参包含三个参数

入参：[ [source, sender,receiver,destination] ，[xml]，[xml2layeragg.zip] ;

2，返回数值包含四个参数

返回值：[[执行结果，压缩子文件数量，size]，[执行结果]，[执行结果]，[执行结果，解压缩文件总数，成功处理子文件数量] ] 。

### 5.1.6 其他约束

​     在本项目中，处理输入文件，输出文件以为，中间的处理过程中的临时文件，**要求在内存中处理**，减少不必要的磁盘读写操作。



## 5.2面向对象与文件对象

### 5.2.1访问对象的句柄方案

​          由于[reference](https://www.baidu.com/s?wd=reference&tn=24004469_oem_dg&rsv_dl=gh_pl_sl_csd)类型在Java虚拟机规范里只规定了一个指向对象的引用，并没有定义这个引用应该通过哪种方式去定位，以及访问到Java堆中的对象的具体位置，因此不同虚拟机实现的对象访问方式会有所不同，主流的访问方式有两种：**使用句柄**和**直接指针。**

![5-2 java 句柄访问](/assets/python-bigdata-starter/cp05/5-2 java 句柄访问.png)

​         图 5-2 使用句柄访问对象

### 5.2.2 Python中对象创建方式

​        定义完python类之后，就可以创建对象。当一个对象被创建后，包含3个方面的特性：对象的句柄、属性和方法。对象的句柄用于区分不同的对象，当对象被创建后，该对象会获取一块存储空间，存储空间的地址即为对象的标识。

​         在python的文件读写操作中，也会涉及到相关的文件对象，也可以成为句柄。

​        在python中，文件的读写操作也是经过高度封装的，相关的对象模型见下图：

   ![5-3 python open function](/assets/python-bigdata-starter/cp05/5-3 python open function.png)

​        图 5-3 python中的open函数与文件对象

​         注意文件对象和文件的关系。对象肯定是在内存，有它的方法和属性，但在内存中没有全部文件，有句柄，文件中当前读取位置等。这里文件还是在硬盘中，但是有一个文件对象作为桥梁。可以实现内存和文件的连接。换句话说，通过“文件对象”屏蔽了底层的细节，方便应用程序进行编程操作。

​       这个对象模型，在本章节的学习中，大部分的操作，都可以参考这个对象关系图。由于python崇尚简洁，所以大量使用了多态的特性，简化了相关的操作。



## 5.3文件读写操作

​       读写文件是最常见的IO操作。Python内置了读写文件的函数，用法和C是兼容的。由于在本章节的用例模型中c涉及到非常多的文件操作，所以我们要“温故而知新”，深度的理解一下文件操作的要点。

​        读写文件前，我们先必须了解一下，在磁盘上读写文件的功能都是由操作系统提供的，现代操作系统不允许普通的程序直接操作磁盘，所以，读写文件就是请求操作系统打开一个文件对象（通常称为文件描述符），然后，通过操作系统提供的接口从这个文件对象中读取数据（读文件），或者把数据写入这个文件对象（写文件）。

### 5.3.1读文件

​        要以读文件的模式打开一个文件对象，使用Python内置的`open()`函数，传入文件名和标示符：

```
>>> f = open('/Users/michael/test.txt', 'r')
```

标示符'r'表示读，这样，我们就成功地打开了一个文件。

F是函数open（）的返回值，在这里f是句柄， 句柄就是file对象。

​        如果文件不存在，`open()`函数就会抛出一个`IOError`的错误，并且给出错误码和详细的信息告诉你文件不存在：

```
>>> f=open('/Users/michael/notfound.txt', 'r')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
FileNotFoundError: [Errno 2] No such file or directory: '/Users/michael/notfound.txt'
```

​       如果文件打开成功，接下来，调用`read()`方法可以一次读取文件的全部内容，Python把内容读到内存，用一个`str`对象表示：

```
>>> f.read()
'Hello, world!'
```

​       最后一步是调用`close()`方法关闭文件。文件使用完毕后必须关闭，因为文件对象会占用操作系统的资源，并且操作系统同一时间能打开的文件数量也是有限的：

```
>>> f.close()
```

​       由于文件读写时都有可能产生`IOError`，一旦出错，后面的`f.close()`就不会调用。所以，为了保证无论是否出错都能正确地关闭文件，我们可以使用`try ... finally`来实现：

```
try:
    f = open('/path/to/file', 'r')
    print(f.read())
finally:
    if f:
        f.close()
```

​      但是每次都这么写实在太繁琐，所以，Python引入了`with`语句来自动帮我们调用`close()`方法：

```
with open('/path/to/file', 'r') as f:
    print(f.read())
```

​      这和前面的`try ... finally`是一样的，但是代码更佳简洁，并且不必调用`f.close()`方法。

​     调用`read()`会一次性读取文件的全部内容，如果文件有10G，内存就爆了，所以，要保险起见，可以反复调用`read(size)`方法，每次最多读取size个字节的内容。另外，调用`readline()`可以每次读取一行内容，调用`readlines()`一次读取所有内容并按行返回`list`。因此，要根据需要决定怎么调用。

​      如果文件很小，`read()`一次性读取最方便；如果不能确定文件大小，反复调用`read(size)`比较保险；如果是配置文件，调用`readlines()`最方便：

```
for line in f.readlines():
    print(line.strip()) # 把末尾的'\n'删掉
```



### 5.3.2 file-like Object

​        像`open()`函数返回的这种有个`read()`方法的对象，在Python中统称为file-like Object。除了file外，还可以是内存的字节流，网络流，自定义流等等。file-like Object不要求从特定类继承，只要写个`read()`方法就行。

`StringIO`就是在内存中创建的file-like Object，常用作临时缓冲。



### 5.3.3写文件

​       写文件和读文件是一样的，唯一区别是调用`open()`函数时，传入标识符`'w'`或者`'wb'`表示写文本文件或写二进制文件：

```
>>> f = open('/Users/michael/test.txt', 'w')
>>> f.write('Hello, world!')
>>> f.close()
```

​      你可以反复调用`write()`来写入文件，但是务必要调用`f.close()`来关闭文件。当我们写文件时，操作系统往往不会立刻把数据写入磁盘，而是放到内存缓存起来，空闲的时候再慢慢写入。只有调用`close()`方法时，操作系统才保证把没有写入的数据全部写入磁盘。忘记调用`close()`的后果是数据可能只写了一部分到磁盘，剩下的丢失了。所以，还是用`with`语句来得保险：

```
with open('/Users/michael/test.txt', 'w') as f:
    f.write('Hello, world!')
```

​      要写入特定编码的文本文件，请给`open()`函数传入`encoding`参数，将字符串自动转换成指定编码。



### 5.3.4 Read and write strings as files

​         在前面的章节5.2.2中介绍过，   像`open()`函数返回的这种有个`read()`方法的对象，在Python中统称为file-like Object。除了file外，还可以是内存的字节流，网络流，自定义流等等。file-like Object不要求从特定类继承，只要写个`read()`方法就行。

`    StringIO`就是在内存中创建的file-like Object，常用作临时缓冲。在大数据处理过程中，由于机器的资源非常有限，所以很多文件操作，都希望在内存中完成，这样可以消除IO的操作瓶颈，这样就可以极大的提升任务的速度。Python提供了StringIO — Read and write strings as files来实现这个功能。

​        This module implements a file-like class, [**StringIO**](https://docs.python.org/2/library/stringio.html#StringIO.StringIO), that reads and writes a string buffer (also known as *memory files*). See the description of file objects for operations (section [File Objects](https://docs.python.org/2/library/stdtypes.html#bltin-file-objects)). (For standard strings, see [**str**](https://docs.python.org/2/library/functions.html#str) and [**unicode**](https://docs.python.org/2/library/functions.html#unicode).)

​      很多时候，数据读写不一定是文件，也可以在内存中读写。

​        StringIO顾名思义就是在内存中读写str。

​      要把str写入StringIO，我们需要先创建一个StringIO，然后，像文件一样写入即可



## 5.4     设计模型（design model）

![5-4 object mode of compress](/assets/python-bigdata-starter/cp05/5-4 object mode of compress.png)

​        图 5-4 设计模型

​       在本实验中，核心是要实现文件的压缩和解压操作， 理论上这种操作当然是可以直接通过文件流来处理，但是反复的文件操作，会导致IO操作的瓶颈，所以，我们考虑在内存中实现这种文件的操作处理。



### 5.4.1 压缩文件的对象详解

![5-5 object model of zip](/assets/python-bigdata-starter/cp05/5-5 object model of zip.png)

​                          图5-5  ZipFile()  function

​        从上图可见，要实现压缩操作，首先要能够使用ZipFile（）函数， 他可以实现从文件对象的压缩操作。而在这个case中，还涉及到双层压缩的问题，所以还处理起来稍微有一些不同，下面将处理的过程做一个简要的说明：

1，首先获取期望生成的压缩文件的名字，也就是zipfilename。

2，调用Zipfile()函数，生成一个文件对象myzip，这里myzip是指向即将在文件系统生成的压缩文件的句柄。

3，根据列表filelist中的元素，提取第一个文件名字的信息结构filename。也就是启动对第一个文件进行处理。

3，创建一个内存中的子压缩文件，subzipfile = io.BytesIO()  #定义子压缩文件的内存file-like对象，实现无落地文件多层压缩。 前面说过，除了file外，还可以是内存的字节流，网络流，自定义流等等。file-like Object不要求从特定类继承，只要写个`read()`方法就行。

4，调用Zipfile()函数，生成一个内存文件对象subzip，这里subzip是指向即将在内存中生成的压缩文件的句柄。

5，subzip.write(filename, arcname=os.path.basename(filename)) 调用内存文件subzip()的write（）方法，#将xml文件加入到subzip内存对象。

6，subzipfile.seek(0) ，调用内存文件subzipfile的seek()方法，移动内存文件的指针位置。

7，生成压缩子文件的名字。

8，将内存中的#将内存中的subzip写入zip文件，这里需要在压缩包myzip中新建一个压缩子文件。

9，关闭内存中的subzip文件。

10，重复上面的操作，直到列表filelist中的全部的文件都完成上面的操作。

11，返回操作的结果。





### 5.4.2压缩文件的流转换过程

![5-6 stream transf](/assets/python-bigdata-starter/cp05/5-6 stream transf.png)

​                                 图5-6 压缩文件的流转换过程

​        根据章节5.5.1 中介绍的的流转换过程，我们可以发现在进行双层压缩的处理过程中，关于子压缩文件的处理，是一个难点，子压缩文件是一个临时文件，假如我们还是使用文件系统中的文件来实现子压缩文件的话，必然会有大量的IO读写，非常的不经济，所以就考虑使用file-like-object来实现子压缩文件。 而这个subzipfile也提供了和文件系统中具体文件一样的行为（behaivor），唯一的区别是在内存中操作会比较敏捷。

在上面的处理过程中，我们使用两个句柄对象，一个是myzip，一个是subzip， 区别是一个是指向文件系统中的双层压缩文件，另外一个指向内存中的子压缩文件。其中文件系统中的双层压缩文件会一直存活，而内存中的子压缩文件，会不断的创建，传递，移动指针，关闭，释放。

备注1：句柄对象的写方法。

上面有两个方法比较类似，注意区分。

·         write指的是将已经存在的文件复制到压缩包，包括路径中的所有文件夹和其下的文件。

·         writestr是直接在压缩包里新建文件夹和文件，data参数是往该文件中写入的内容。

**备注2：subzipfile.seek(0)**

**这里的操作是将读取位置调整到初始位置。**

在本case中，仅仅需要压缩3个文件，所以用于临时处理目的的子文件无论是在文件系统中，还是在内存中，并不会有很大的差异，但是假如我们需要处理100个文件进行压缩， 那么100次文件的打开，关闭操作，在内存中处理会有很大的效率优势。在大数据的处理过程中，因为计算资源的稀缺性，所以必须要尽量的节约计算资源，所以在内存中处理文件是很重要的技能。

### 5.4.3 解压文件的对象详解

![5-7 object model of unzip](/assets/python-bigdata-starter/cp05/5-7 object model of unzip.png)

​       图 5-7 基于递归调用的解压缩函数

​      从上图可见，由于多层压缩文件，是无法事先知晓压缩的层次，所以使用常规的处理方式就比较麻烦，所以在这里考虑采用递归调用的方法，也就是说，要递归调用unzip（FN）。

在这个应用中

```
subzip = io.BytesIO(myzip.read(filename))
```

subzip是一个**file-like object**，在这里有一个分支判断：

1，如果识别到的子文件的扩展名为zip格式，则创建一个subzip，并将文件流传递到subzip中，将subzip再次传递给函数Unzip（FN），进行递归调用。

2，如果识别到子文件的扩展名为非zip 格式，则直接使用myzip.extract(filename,distdir) 函数，实现解压缩处理。并生成一个具体的解压缩文件。



## 5.5总结

### 5.5.1结果演示

![cp05 test result](/assets/python-bigdata-starter/cp05/cp05 test result.png)

​       图 5-8 结果演示

​         从上图可见，相关的代码可以轻松的实现文件的压缩和解压缩的操作。在这里轻松实现了内存中的文件操作

###  5.5.2讨论

1，本案例中深度讨论了文件系统的open（）函数，方便应用程序透过对象化的方式来访问文件系统中的文件。

2，本案例讨论了利用压缩文件管理的zipfile（）函数，方便文件程序透过对象化的方案进行文件压缩，和解压操作。

3、利用**file-like Object**可以轻松实现内存中的文件处理。

4，在解压操作中，使用递归调用，保障了应用程序良好的扩展性。

5，压缩，和解压缩过程中，使用了大量的list操作，简化了代码的编写。
