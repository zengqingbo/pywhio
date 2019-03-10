---
title: "Python BigData Starter CP11"
cover: "3.jpg"
category: "python"
date: "2019-03-10"
slug: "python-bigdata-starter-cp11"
tags:
---​

# 11   高性能FTP文件下载

经过了前10章的学习，我们基本上已经胜任了常规的数据处理任务，已经可以胜任基本的storage，data，information的处理任务，相关任务处理的模型见下图：

![11-1 数据应用的层次](/assets/python-bigdata-starter/cp11/11-1 数据应用的层次.png)

​                              11-1 数据处理的四个层次

假如时光倒退10年，能够做完前面的8道题目，基本上就可以投身到数据仓库掘金者的行列。 然而时光已经到了2019年，随着物联网，5G网络，工业4.0的流行，数据的规模极大的扩展，因为数据的规模大到常规的技术手段无法处理，所以我们必须扩展我们的知识结构，掌握分布式，并行计算的一些方法和手段。

分布式并行计算也并不算是很新的技术，但是早期由于开源软件尚不发达，要使用这些技术往往需要借助非常昂贵的中间件，一般的人很难有上手的机会，所以这个技术显得非常的神秘。然而感谢万能的GIT，现在已经有大量的开源分布式框架，可以用于相关的方案的实施。

在这个模块中，我们会从分布式并行计算的基础开始，通过几个原型实验，理解分布式计算的基本设计思想。



## 11.1     问题背景介绍

### 11.1.1 新用例故事介绍

​    ![11-2 高速FTP下载用例故事](/assets/python-bigdata-starter/cp11/11-2 高速FTP下载用例故事.png)

​                             11-2 FTP 下载用例故事



从上图可见，1个市级的银行，至少有上百个营业网点，那么要每15分钟内尽量快的把每个营业网点的文件获取回来，这种情况下，效率问题就显得很重要。我们要能够通过并行化的方式，尽量快的将文件提取到中心机房。



### 11.1.2 输入数据处理（I）

​       输入数据为[配置文件URL，buffer_path]

​      此处的输出有两个对象分别是

​     1，配置文件URL： 这里给出的是FTP的配置文件的绝对路径，这里配置文件采用.py 的方式进行配置。

2,buffer_path: 定义一个绝对路径，用于保存采集到的文件。

 假如不提供这个指标，默认的情况下就是根据配置文件来处理。

配置文件的示例为：

   ![11-x ftp_cfg](/assets/python-bigdata-starter/cp11/11-x ftp_cfg.png)



​     考虑到并行化的要求，此处至少有10个FTP服务器。



### 11.1.3 处理过程（P）

下面简要的说明一下相关的处理过程

   1，用户输出一个输入参数，含配置文件的绝对路径（必选参数），以及期望采集的FTP服务器的列表（可选参数）。

   2，应用程序，引入配置文件，生成待采集服务器的列表。

   3，应用程序待采集服务器的列表，预先fork多个FTP采集实例。并根据当前可用的资源，顺序执行采集实例。

   4，应用程序从其中提取一个配置信息，尝试连接FTP服务器，如果无法连接，则返回失败信息。

   5，如果连接成功，就根据配置文件中的path参数，访问FTP路径，遍历FTP路径下的全部文件，形成一个待采集的file list。

6，应用程序待采集服务器的列表，预先fork多个FTP文件下载实例。此处设定为5个

6，应用程序根据参数buffer_path， 在对于路径下创建一个文件夹，文件夹的名字为和当前采集的服务器的名字相同，比如server1。

7， 应用程序根据待采集的file list，并行提取文件，并拷贝到对弈的目录下。

8，全部文件拷贝成功以后，就返回一个结果集。信息为

[serve_name, result, number of file, buffer_path ，[[file1,size],[file2,size],…[filen,size]]]

其中server_name ： 用于表达本次采集，采集的服务器的名称。

​     Result： 用于记录本次采集的成果，其中0，成功；1 ，失败。

​     Number of file： 本次采集的文件数量。

​     Buffer path： 本次采集文件存在的路径。

   File list ：本次采集文件的文件列表。每个元素也是一个列表，记录文件名，文件大小。

9，应用程序完成1个FTP文件的下载以后， 关闭线程池，释放CPU资源。

​    10，应用程序根据预fork的采集实例，完成后续的采集工作。

 10，应用程序返回本次采集多个FTP服务器的的结果集。

1.1.4 处理结果(o)

1,文件拷贝： 将FTP服务器下的目录中的文件，拷贝到本机的路径下。

2，返回结果集。

Result={“server1”: [result, number of file, buffer_path ，[[file1,size],[file2,size],…[filen,size]],

​     “server2”: [result, number of file, buffer_path ，[[file1,size],[file2,size],…[filen,size]] }



## 11.2     并发处理原理

### 11.2.1  “多任务”的操作系统基础

　　大家都知道，操作系统可以同时运行多个任务。比如你一边听音乐，一边聊IM，一边写博客等。现在的cpu大都是多核的，但即使是过去的单核cpu也是支持多任务并行执行。



　　单核cpu执行多任务的原理：操作系统交替轮流地执行各个任务。先让任务1执行0.01秒，然后切换到任务2执行0.01秒，再切换到任务3执行0.01秒...这样往复地执行下去。由于cpu的执行速度非常快，所以使用者的主观感受就是这些任务在并行地执行。



　　多核cpu执行多任务的原理：由于实际应用中，任务的数量往往远超过cpu的核数，所以操作系统实际上是把这些多任务轮流地调度到每个核心上执行。

　　对于操作系统来说，一个应用就是一个进程。比如打开一个浏览器，它是一个进程；打开一个记事本，它是一个进程。每个进程有它特定的进程号。他们共享系统的内存资源。进程是操作系统分配资源的最小单位。

　　而对于每一个进程而言，比如一个视频播放器，它必须同时播放视频和音频，就至少需要同时运行两个“子任务”，进程内的这些子任务就是通过线程来完成。线程是最小的执行单元。一个进程它可以包含多个线程，这些线程相互独立，同时又共享进程所拥有的资源。

### 11.2.2 线程

   参考 https://www.cnblogs.com/whatisfantasy/p/6440585.html

##### 11.2.2.1 什么是线程

**线程**是操作系统能够进行运算调度的最小单位。它被包含在进程之中，是进程中的实际运作单位。一条线程指的是进程中一个单一顺序的控制流，一个进程中可以并发多个线程，每条线程并行执行不同的任务。一个线程是一个execution context（执行上下文），即一个cpu执行时所需要的一串指令。

##### 11.2.2.2 线程的工作方式

假设你正在读一本书，没有读完，你想休息一下，但是你想在回来时恢复到当时读的具体进度。有一个方法就是记下页数、行数与字数这三个数值，这些数值就是execution context。如果你的室友在你休息的时候，使用相同的方法读这本书。你和她只需要这三个数字记下来就可以在交替的时间共同阅读这本书了。

线程的工作方式与此类似。CPU会给你一个在同一时间能够做多个运算的幻觉，实际上它在每个运算上只花了极少的时间，本质上CPU同一时刻只干了一件事。它能这样做就是因为它有每个运算的execution context。就像你能够和你朋友共享同一本书一样，多任务也能共享同一块CPU。

### 11.2.3  进程

一个程序的执行实例就是一个**进程**。每一个进程提供执行程序所需的所有资源。（进程本质上是资源的集合）

一个进程有一个虚拟的地址空间、可执行的代码、操作系统的接口、安全的上下文（记录启动该进程的用户和权限等等）、唯一的进程ID、环境变量、优先级类、最小和最大的工作空间（内存空间），还要有至少一个线程。

每一个进程启动时都会最先产生一个线程，即主线程。然后主线程会再创建其他的子线程。

与进程相关的资源包括:

·         内存页（**同一个进程中的所有线程共享同一个内存空间**）

·         文件描述符(e.g. open sockets)

·         安全凭证（e.g.启动该进程的用户ID）

### 11.2.4 进程与线程区别

1.同一个进程中的线程共享同一内存空间，但是进程之间是独立的。
 2.同一个进程中的所有线程的数据是共享的（进程通讯），进程之间的数据是独立的。
 3.对主线程的修改可能会影响其他线程的行为，但是父进程的修改（除了删除以外）不会影响其他子进程。
 4.线程是一个上下文的执行指令，而进程则是与运算相关的一簇资源。
 5.同一个进程的线程之间可以直接通信，但是进程之间的交流需要借助中间代理来实现。
 6.创建新的线程很容易，但是创建新的进程需要对父进程做一次复制。
 7.一个线程可以操作同一进程的其他线程，但是进程只能操作其子进程。
 8.线程启动速度快，进程启动速度慢（但是两者运行速度没有可比性）。



### 11.2.5

​      线程和进程的操作是由程序触发系统接口，最后的执行者是系统，它本质上是操作系统提供的功能。而协程的操作则是程序员指定的，在python中通过yield，人为的实现并发处理。

协程存在的意义：对于多线程应用，CPU通过切片的方式来切换线程间的执行，线程切换时需要耗时。协程，则只使用一个线程，分解一个线程成为多个“微线程”，在一个线程中规定某个代码块的执行顺序。

协程的适用场景：当程序中存在大量不需要CPU的操作时（IO）。
 常用第三方模块gevent和greenlet。（本质上，gevent是对greenlet的高级封装，因此一般用它就行，这是一个相当高效的模块。）

## 11.3     并行化组件Joblib

​      Python原生就支持多进程，多线程处理。 然而这本教程是作为python的大数据处理的入门教程，所以我们计划介绍一个python的高级组件Joblib.

在python的世界中，Joblib是一个网红组件，他可以用于numpy的数据处理，也可以用于sick-learn的机器学习处理过程，是一个非常强大的大数据处理组件，而在本章节中，我们仅仅使用到其中最基础的并行化处理的功能。

 ![11-1 python joblib](/assets/python-bigdata-starter/cp11/11-1 python joblib.png)



### 11.3.1 简介

​    Joblib是一组工具集，用于支持在python的编程环境中提供轻量化的数据流水线处理的功能。他特别适用以下的应用场景。

1、面向函数提供透明的磁盘缓存功能，以及简化版本的重新运算功能（re-evaluation），这种编程方案也常常被称为是内存模式。(memoize pattern)

2、简单容易的并行计算功能更。

   Joblib针对数据并行化处理进行了特别的优化，以提升处理效率和稳定性，所以特别适合大数据处理的应用场景，尤其针对numpy arrays进行了特别的优化。



### 11.3.2  Pipelines 处理模式

   Pilelines处理模式是早期unix系统的一种处理模式，而在今天的大数据处理的项目中，Pipelines 处理模式也成为一种非常重要的编程模式。

​      在一个基于unix操作系统的哦计算机中，管道 **pipeline**是一种使用消息传递进行进程间通信的机制。管道实质是是一组透过标准的stream对象链式连接的进程。这样，因此每个进程（stdout）的输出文本直接作为输入（stdin）传递给下一个进程。 第一个进程在第二个进程启动之前未完成，但它们是同时执行的。在Unix的开发过程中，道格拉斯·麦克罗伊（Douglas McIlroy）在Unix的发源地的贝尔实验室大力提倡了“管道”概念，它通过类比物理管道命名。 这些管道的一个关键特征是它们“隐藏内部hiding of internals"”（Ritchie＆Thompson，1974）。 在这种机制下使得系统更清晰和简单。

​                    ![11-3 pipeline处理模式](/assets/python-bigdata-starter/cp11/11-3 pipeline处理模式.png)

​                      图 11-3 A pipeline of three program processes run on a text terminal

   上图是一个关于unix系统中的文本终端操作的一个案例，他使用的是一种叫做[anonymous pipes](https://en.wikipedia.org/wiki/Anonymous_pipe)的机制，在这种机制下前一个进程写的数据会通过操作系统进行缓存，直到下一个进程读出相关的进程为止， 这个单向的管道会在所有进程结束时消失。。

匿名管道的标准shell语法是列出多个命令，用竖线分隔

```
 process1 | process2 | process3
```

举例来说，要显示当前目录下的所有文件([ls](https://en.wikipedia.org/wiki/Ls)) ，过滤出ls output中 包含了字符串"key"([grep](https://en.wikipedia.org/wiki/Grep))，并在滚动页面（更少）中查看结果，用户在终端的命令行中键入以下内容：

```
ls -l | grep key | less
```



“ls -l”产生一个进程，其输出（stdout）通过管道输出到“grep key”进程的输入（stdin）; 同样适用于“少”的过程。 每个进程都从前一个进程获取输入，并通过标准流为下一个进程生成输出。 每个“|”告诉shell通过（匿名）管道的进程间通信机制，将左侧命令的标准输出连接到右侧命令的标准输入。 管道是单向的; 数据从左到右流过管道。

### 11.3.3 Pipelines操作的优势

基于Pipeline processing systems可以提供一系列的优势：

1提升Data-flow programming的效率

·         **On-demand computing****（按需计算）****:** 在基于基于管道处理模式的系统（如labView或VTK）中，输出只在输入发生变化时才会根据需要执行计算。

·         **Transparent parallelization****（透明并行化）****:** 可以检查管道拓扑以推断可以并行运行哪些操作（它等同于纯函数编程）。(it is equivalent to purely functional programming).

2溯源跟踪以便更好的理解代码：

·         **跟踪数据和计算步骤****Tracking of data and computations:** 这使得计算实验的再现性成为可能。

·         **检查****data flow:** 检查中间结果有助于调试和理解。

### 11.3.4 Joblib的设计哲学

​    函数是每个人使用的最简单的抽象。 Joblib中的管道作业（或任务）由装饰函数组成。

​    Pipeline jobs (or tasks) in Joblib are made of decorated functions.

以有意义的方式来跟踪参数需要规范数据模型。 Joblib放弃了它并使用散列来提高性能和稳健性。





## 11.4     嵌入式并行化方案

### 11.4.1 应用 概述

Joblib provides a simple helper class to write parallel for loops using multiprocessing. The core idea is to write the code to be executed as a generator expression, and convert it to parallel computing:

Joblib提供一个简单的帮助类，针对一些列表遍历处理的场景来配置多进程的处理代码。核心思想是编写要作为生成器表达式执行的代码，并将其转换为并行计算。 以下是一个简单的例子：

针对一个列表中的10个数进行先平方再开方运算。（i**2  是平方，  sqrt()是开方，  等于不运算）

```
>>> from math import sqrt
>>> [sqrt(i ** 2) for i in range(10)]
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
```

我们利用joblib的并行化处理，就可以很方便的利用2个CPU进行处理:

```
>>> from math import sqrt
>>> from joblib import Parallel, delayed
>>> Parallel(n_jobs=2)(delayed(sqrt)(i ** 2) for i in range(10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
```

**正如官方文档看到的，   Parallel(n_jobs=1)(delayed(sqrt)(i**2) for i in range(10)) 运行结果就是1个list。**

**使用列表生成式输入了  delayed(sqrt)(i**2) for i in range(10)   给 Parallel()(input)**

   我们再看看上面的语句：

delayed(sqrt)(i**2) for i in range(10)

相应的执行以后获得一个生成器：

![11-X 列表生成器](/assets/python-bigdata-starter/cp11/11-X 列表生成器-1552183035806.png)

而我们进一步打印其中的列表：

![11-X 列表生成器结果](/assets/python-bigdata-starter/cp11/11-X 列表生成器结果-1552183067333.png)

观察其中的一个元素

![11-X 输入列表元素](/assets/python-bigdata-starter/cp11/11-X 输入列表元素-1552183085574.png)

()是位置参数，  i**2就是i平方，入参(),{} 是接受任意参数的方法。这里打印出来的是函数头，并没有运行。

利用这些输入的列表中，每个元素都是可以后续用于进行并行计算。实现 Embarrassingly parallel for loops

### 11.4.2 基于线程的并行化和基于进程的并行化

​        默认情况下，joblib.Parallel使用'loky'后端模块启动单独的Python工作进程，以便在不同的CPU上同时执行任务。 这是通用Python程序的合理默认值，但由于输入和输出数据需要在一个队列中进行序列化处理以与生产者进程（worker processes）通信，因此可能会产生显著的开销。

当你知道你当前调用的函数是基于一个编译扩展，他在其大部分计算期间释放Python全局解释器锁（GIL），那么并发生产者使用线程机制的会比使用进程机制会工作的更加有效率。例如，如果您使用Cython函数的nogil块编写代码的CPU密集部分，就比较适合应用这种情况

当您知道您正在调用的函数基于在其大部分计算期间释放Python全局解释器锁（GIL）的已编译扩展时，则使用线程而不是Python进程作为并发工作程序更有效。 例如，如果您使用Cython函数的nogil块编写代码的CPU密集部分，就会出现这种情况。

要提示您的代码可以有效地使用线程，只需将prefer =“threads”作为joblib.Parallel构造函数的参数传递。 在这种情况下，joblib将自动使用“线程”后端而不是默认的“loky”后端:

```
>>> Parallel(n_jobs=2, prefer="threads")(
...     delayed(sqrt)(i ** 2) for i in range(10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
```

也可以在上下文管理器的帮助下手动选择特定的并行处理后端实现：:

```
>>> from joblib import parallel_backend
>>> with parallel_backend('threading', n_jobs=2):
...    Parallel()(delayed(sqrt)(i ** 2) for i in range(10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
```



It is also possible to manually select a specific backend implementation with the help of a context manager:

Note：  parallel_backend context manager.

### 11.4.3 上下文管理器

  首先，什么是上下文管理器？上下文管理器就是实现了上下文管理协议的对象。主要用于保存和恢复各种全局状态，关闭文件等，上下文管理器本身就是一种装饰器。
 下面我们就通过一下几个部分，来介绍下上下文管理器。

**1、上下文管理协议(context management protocol)**

上下文管理协议包括两个方法：

·         `contextmanager.__enter__()` 从该方法进入运行时上下文，并返回当前对象或者与运行时上下文相关的其他对象。如果with语句有as关键词存在，返回值会绑定在as后的变量上。

·         `contextmanager.__exit__(exc_type, exc_val, exc_tb)` 退出运行时上下文，并返回一个布尔值标示是否有需要处理的异常。如果在执行with语句体时发生异常，那退出时参数会包括异常类型、异常值、异常追踪信息，否则，3个参数都是None。

**2 、with语句**

说上下文管理器就不得不说with语句，为什么呢？
 因为with语句就是为支持上下文管理器而存在的，使用上下文管理协议的方法包裹一个代码块（with语句体）的执行，并为try...except...finally提供了一个方便使用的封装。
 with语句的语法如下：

```
with EXPR as VAR:
    BLOCK
```

with和as是关键词，EXPR就是上下文表达式，是任意表达式（一个表达式，不是表达式列表），VAR是赋值的目标变量。"as VAR"是可选的。

这样with语句的执行过程简单表达如下。

1. 执行上下文表达式，获取上下文管理器

2. 加载上下文管理器的__exit__()方法以备后期调用

3. 调用上下文管理器的__enter__()方法

4. 如果with语句有指定目标变量，将从__enter__()方法获取的相关对象赋值给目标变量

5. 执行with语句体

6. 调用上下文管理器的__exit__()方法，如果是with语句体造成的异常退出，那异常类型、异常值、异常追踪信息将被传给__exit__()，否则，3个参数都是None。


 链接：https://www.jianshu.com/p/7bae11eaf84d



## 1.5     设计模型

### 1.5.1 多进程FTP下载方案：

   由于本次的服务器中，需要尽快从多个外部的FTP服务器中下载文件，所以连接外部FTP服务器，采用了多进程的方案



​             ![11-2多进程下载](/assets/python-bigdata-starter/cp11/11-2多进程下载.png)

​                                                 图 11-4 多进程方案
 从上图可见，主要的对象结构包含4部分：

1，FTP下载函数 ftp_getter: 用于访问外部的FTP服务器，获取待采集路径下的文件列表，生成filelist， 然后调用函数mt_getter()进行文件下载。这是整个模块中的原子函数。

2、生成输入looplist：使用使用列表生成式输入了  (delayed(ftp_getter)(ftp_cfg) for ftp_cfg in ftps_cfg.values() )  给 Parallel()(input)。

3、并行化模块Parallel()：包含并行化parallel对象，和相关的上下文管理器。

4、输出list：返回了1个list  ，就是status， 就是对每个ftps_cfg中的ftp_cfg 多进程执行了 ftp_getter返回结果的list



### 11.5.2 多线程下载

​    ![11-3 多线程下载](/assets/python-bigdata-starter/cp11/11-3 多线程下载.png)

​                       图 11-5 多线程方案

上图是多线程的文件下载对象模型， 基本上并行化的方式和上面

的多进程多FTP服务器下载方案基本一样。所以在此就不详述了。



## 1.6     讨论

​     利用joblib强大的工具集，可以很方便的进行多进程，多线程的配置，运行的结果见下图：

   ![cp11 result](/assets/python-bigdata-starter/cp11/cp11 result.png)

1）joblist 提供了强大的工具集，方便进行并行化处理。

2）并行化处理的核心是能够生成1个list，实现loop操作。也就是经典的Embarrassingly parallel for loops。

3）通常在生成这个输入的list的时候，要使用列表生成式。
