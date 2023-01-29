## MapReduce
完成粗糙的代码不难，但比较容易踩坑  
![](imgs/mr-1.png)
### 整体设计
我的设计好像和大部分人不同。lab的本意应该是worker里循环调用cd（下文简称cd）的rpc接口，拉取任务来执行；cd对worker的状态不感知，worker也没有对外暴露的rpc接口。然而我受了论文里描述的影响，让worker也起了rpc接口，cd调用worker的接口推送任务。  
在这个lab上，两种实现应该是差不多的。不过个人觉得cd推的模式更好，方便管理和感知worker，再例如如果后续优化，cd可以把任务推给最合适的worker（比如论文里说的"距离最近"/"离保存了文件的机器最近的文件"）⬅️但我还没看过gfs的论文，这也是我瞎说的。  
流程大概如下：
1. worker调用registerWorker，往cd里注册worker
2. cd维护一个存储了空闲worker的阻塞队列，循环获得worker后调用map或reduce对应的rpc接口。
3. map时，处理文件中的内容。再根据key的hash值，把中间结果分到不同的中间文件中。中间文件命名格式：mr-X-Y（X是map任务编号，Y是reduce任务编号）。
4. map后的shuffle阶段中（在cd上进行），整理中间结果，也就是把mr-X-Y中Y相同的文件放在一个数组中。该数组会作为参数传给reduce接口。 
5. reduce阶段没什么好说的，就是处理中间文件，生产结果文件mr-out-X（X是reduce任务编号）。
6. 整理reduce阶段产生的文件，删除无用/多余的文件，重命名一些文件。
7. 结束时，调用接口通知每个worker关闭。
### 重要的细节设计
主要是error tolerance这块。伪代码如下：
```` go
for task := range tasks {
	go 某个空闲的worker.hanlde(task)
	if 超时后worker仍没处理完 {
	    go 另个空闲的worker.handle(task)
	}
	if 两个worker有任意一个处理完了 {
	    结束
	} else if 超时后两个worker都处理不完 {
	    丢弃该任务
	}
}
````
1. 对某个worker处理任务失败时，可以进行重试。目前设置重试是2次。这个重试是不为任务纬度的循环所感知的。
2. 对某个已被分配的任务，如果worker处理的很慢，就再找个worker帮忙，有一个worker干完了就行。  

具体实现上：
1. 主线程和worker.handle协程间用channel来通信。
2. 不同worker间通过原子变量来通信是否已经有worker干完了。原子变量用cas来修改，cas修改成功的协程才能往channel里写结果。所以只会有一个协程能写入结果。
本来想用sync.once来取代原子变量的，但sync.once底层用了lock，效率不够好。

map任务重试时，直接去做就好，因为每个worker生产的中间文件都放在各自的文件夹里，互不可见，名称不会冲突，最后传给reduce的只会有一个文件。  
reduce任务需要考虑写结果文件时的冲突，避免相互覆盖/重复写的情况。我的处理比较简单，就是给重做的reduce任务赋一个新的reduce任务编号，并记录下（原始任务编号，重做任务编号）和最后执行成功的任务编号。全部reduce任务跑完后，把不要的文件删掉，正确的文件命名为【原始任务编号对应的文件名】就行。
### 吐槽&踩过的坑
1. 多个worker并行时，socket名称可能冲突。开始用了time.now.nanoseconds，但还是有冲撞的概率，改用pid做后缀解决。os学的太烂（。开始没想到pid，折腾了老半天。
2. 开始我让一个worker上跑了多个协程，然后有test没通过。没考虑同一机器上不同task可能冲突的问题（比如建了相同名字的临时文件），毕竟func是用户指定的。不过还是感觉一个worker只跑一个协程有点浪费，也许可以通过config文件来指定worker的协程数？
3. channel要初始化。不能写个var ch chan T就完事
4. 还有一些写代码时的小测试放在coordinator_test.go里了
### 值得做的优化
很多地方写的比较粗糙，虽然大概不会去优化，姑且记一下
1. cd和worker里对于status的控制很粗糙，即何时开始接收请求、何时开始处理请求的问题，高并发量下估计会出错
2. input的文件没有调整大小，按照论文的说法，理想的文件大小在16MB-64MB间，应该加个分割大文件的步骤
3. worker和cd退出时都没有考虑正在跑的任务，也许可以利用协程池管理它们？退出时把没跑完的任务也处理下
4. cd对worker的状态感知做的一般。目前仅仅在dial失败时会把worker置为unavailable
5. 心跳检测&last gasp没写。
6. 还有论文里的说法是"map worker的结果数据存在local disk，而reduce worker的数据存在全局文件系统上。所以完成的map task在遇到错误时需要重试；而完成的reduce task不需要。"⬅️如上所述，这里我也不是很理解，我让两个都重试了。