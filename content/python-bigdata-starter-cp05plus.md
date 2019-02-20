---
title: "Python BigData Starter CP05 plus"
cover: "5.jpg"
category: "python"
date: "2019-02-19"
slug: "python-bigdata-starter-cp05plus"
tags:
---



​        文件压缩和解压缩操作在大数据应用领域有非常多的应用场景。其中负责压缩+解压缩操作的函数，和负责打开+关闭文件操作的函数有很大的相似性。在此，我们将相关的知识点再次进行回顾。

##

## 5.6     补充设计文档

### 5.6.1 agg(source,sender)

​     相关的函数用于实现将源文件夹下的*xml 格式的文件，通过两层压缩，放置到sender文件夹下。

 ![RE1](/assets/python-bigdata-starter/cp05/RE1.png)

​      在这里，我们要考虑为临时压缩文件确定一个合理的名字。显然避免重复是一个重点。那么如何确定压缩文件的名字？在本模块中，要考虑采用uuid的组件。

　uuid是128位的全局唯一标识符（univeral unique identifier），通常用32位的一个字符串的形式来表现。有时也称guid(global unique identifier)。python中自带了uuid模块来进行uuid的生成和管理工作。

uuid.uuid3(namespace,name)　　通过计算一个命名空间和名字的md5散列值来给出一个uuid，所以可以保证命名空间中的不同名字具有不同的uuid，但是相同的名字就是相同的uuid了。namespace并不是一个自己手动指定的字符串或其他量，而是在uuid模块中本身给出的一些值。比如uuid.NAMESPACE_DNS，uuid.NAMESPACE_OID，uuid.NAMESPACE_OID这些值。这些值本身也是UUID对象，根据一定的规则计算得出。

uuid.uuid5(namespace,name)　　和uuid3基本相同，只不过采用的散列算法是sha1

| filelist = get_filelist(source, '.xml')                      |
| ------------------------------------------------------------ |
| zipfilename = 'zipfile-%s.zip' %   uuid.uuid5(uuid.NAMESPACE_URL,str(filelist)) |
| zipfilename = os.path.join(sender, zipfilename)              |
| if compress(filelist, zipfilename):                          |
| result = [0, len(filelist), get_FileSize(zipfilename)]       |
| print_result('1 agg','return: %s' % result,'end')            |
| return result                                                |

聚合函数的原理简单说明如下

1， 首先调用函数get_filelist(source, '.xml')，从source目录下识别出后缀为xml格式的文件，将文件列表返回到一个list中。

2，加下来调用uuid模块的uuid5方法，生成一个不重复的文件名。 并添加上一个sender前缀，生成最后的文件名称。

3，调用函数compress(filelist, zipfilename)， 生成压缩文件，其中的入参有两个，1，期待压缩的文件列表，2，期待压缩的文件名称。

  4，生成相关的结果列表，[压缩结果，待压缩的文件数量，压缩文件的大小]

​      5，打印响应结果。



### 5.6.2 recieve(sender,reciever)

  相关的函数用于实现，将文件从sender下拷贝到reciever下。由于涉及到文件的管理，所以需要使用一个新的组件叫做shutil

   The **shutil** module offers a number of high-level operations on files and collections of files. In particular, functions are provided which support file copying and removal. For operations on individual files, see also the **os** module.

​    Shutil 组件实现了对sh脚本操作功能的封装，所以可以提供一种更加高效能的文件操作，尤其是对一个目录下的文件和目录进行处理的情况下，这个组件提供的功能更加强大。

| def recieve(sender,reciever):                                |
| ------------------------------------------------------------ |
| filelist = get_filelist(sender, '.zip')                      |
| print('copy %d file(s) to reciever folder.' % len(filelist)) |
| for filename in filelist:                                    |
| shutil.copy(filename, reciever)                              |
| print('copied file: %s' %   os.path.join(reciever,os.path.basename(filename) ) ) |
| print_result('2 recieve','return: 0','end')                  |
| return 0                                                     |

拷贝函数的功能简要说明如下

1，利用函数get_filelist(sender, '.zip')识别出sender目录下期望拷贝的zip格式的文件。

 2，根据期待发送的文件列表，逐个实现文件的拷贝，使用的方法是

  shutil.copy(filename, reciever)

 3，提供返回结果集。

 ![RE2](/assets/python-bigdata-starter/cp05/RE2.png)

   以上是文件拷贝的结果打印



### 5.6.3 extract(reciever,destination)

解压缩操作负责将前期拷贝的文件，加压缩到目标文件夹中。相关的代码有

| filelist =   get_filelist(reciever, '.zip')                  |
| ------------------------------------------------------------ |
| for filename in filelist :                                   |
| print('decompressing file: %s' % filename)                   |
| unzipresult = decompress(filename,destination)               |
| filenum = len(unzipresult)                                   |
| filedonenum = len([v for v in unzipresult.values() if v == True]) |
| result = [                                                   |
| 0 if filenum==filedonenum else 1 if filedonenum==0 else 2 ,  |
| filenum, filedonenum,                                        |
| ]                                                            |
| print_result('3 extract','return: %s' % result,'end')        |

相关的代码简单解释如下

1，获取receiver路径下的，带有zip格式的文件，并传入到filelist当中。

2，遍历filelist，选择第一个文件，进行解压缩处理。

3，打印期望解压的文件信息。

4，调用函数decompress(filename,destination)，进行解压，并将返回的结果集存入到unzipresult中。

5，生成打印结果。

![RE3](/assets/python-bigdata-starter/cp05/RE3.png)

 ![5-X 补充设计部分](/assets/python-bigdata-starter/cp05/5-X 补充设计部分-1550579849510.jpg)

### 5.6.4  get_filelist(dir, ext)

从上面三个函数的介绍中，我们发现其中有一个公共操作，就是如何从一个特定文件下，扫描特定扩展名的文件，并返回到1个list当中，相关的的函数简要介绍如下。

| filelist = [os.path.join(dir,f) for f in os.listdir(dir)]    |
| ------------------------------------------------------------ |
| filelist = [f for f in filelist if os.path.splitext(f)[-1].lower() ==   ext] |
| return filelist                                              |

相关的操作主要由两步

1，遍历目标路径下的文件，将文件信息，传入到一个列表filelist当中去。

2， 利用列表生成式的功能，将filelist中扩展名和函数入参的传入的文件扩展名进行比对，过滤生成期望处理的文件列表。

在这里，我们可以发现提炼函数的优势，通过提炼核数，这样在多个模块中，都可以方便的复用这个功能。

### 5.6.5 init(dirs)

初始化一个特定文件夹，相关的操作可以实现对特定文件下的全部文件进行删除操作。

代码是：

| def init(dirs):                       |
| ------------------------------------- |
| for d in dirs:                        |
| shutil.rmtree(d,ignore_errors=True)   |
| if not os.path.isdir(d) : os.mkdir(d) |

这一段代码在主函数下有应用：

​    **init([sender,reciever,destination])**

相关代码的作用是初始化四个文件夹， 并对文件夹进行清空处理。





## 5.7     递归编程回顾

在解压缩操作部分，我们还使用了递归的解压缩的操作，下面针对相关的设计方法在简要进行回顾。

![5-13递归编程的模型](/assets/python-bigdata-starter/cp05/5-13递归编程的模型.png)

​                               图 5-13 递归编程的原型

​         在我们了解了递归的基本思想及其数学模型之后，我们如何才能写出一个漂亮的递归程序呢？根据专家经验认为主要是把握好如下三个方面：

**1).** **明确递归终止条件**

　　 我们知道，递归就是有去有回，既然这样，那么必然应该有一个明确的临界点，程序一旦到达了这个临界点，就不用继续往下递去而是开始实实在在的归来。换句话说，该临界点就是一种简单情境，可以防止无限递归。

**2).** **给出递归终止时的处理办法**

　　 我们刚刚说到，在递归的临界点存在一种简单情境，在这种简单情境下，我们应该直接给出问题的解决方案。一般地，在这种情境下，问题的解决方案是直观的、容易的。

**3).** **提取重复的逻辑，缩小问题规模\***

　　  我们在阐述递归思想内涵时谈到，递归问题必须可以分解为若干个规模较小、与原问题形式相同的子问题，这些子问题可以用相同的解题思路来解决。从程序实现的角度而言，我们需要抽象出一个[干净利落](https://www.baidu.com/s?wd=%E5%B9%B2%E5%87%80%E5%88%A9%E8%90%BD&tn=24004469_oem_dg&rsv_dl=gh_pl_sl_csd)的重复的逻辑，以便使用相同的方式解决子问题。

### 5.7.1 解压函数回顾

![5-14 recusive call](5-14 recusive call.jpg)

​                                                                  图 5-14 解压操作回顾

​        回顾上面的三要素，我们来看看

1，递归终止条件：if ext == '.zip'  假如识别的文件扩展名不是zip， 则递归条件终止。

2，递归终止时的处理方法。

 myzip.extract(filename,distdir)   在此时，就执行解压操作。

3，重复逻辑

  从上面的案例中，我们发现执行了四次解压操作，所以这里就是重复的逻辑，提炼为最后的最小处理规模。
