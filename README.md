# 人员行为画像的离线任务

## 主要目的

通过人员的抓拍信息，利用flink定时批处理实现离线算法

## 设计如下

Clickhouse表设计如下，采用MergerTree表引擎

### 每日画像表

| No   | 字段名              | 字段说明       | 类型                               | 备注                                                         |
| ---- | ------------------- | -------------- | ---------------------------------- | ------------------------------------------------------------ |
| 1    | group               | 聚类分组编号   | UInt64                             | 从人员聚类分组表获取的组号                                   |
| 2    | personnel_id_number | 人员身份证号   | String                             |                                                              |
| 3    | occupation          | 人员职业       | UInt8                              | 1:上班族 2：网约车司机 3：出租车司机 4：服务人员/保卫人员    |
| 4    | occupation_prob     | 人员职业可信度 | Float64                            |                                                              |
| 5    | home_locations      | 住宅区域       | Array[Tuple(UInt64,UInt8)]         | [(3702014291461802,8)] 代表该点位出现时间                    |
| 6    | work_locations      | 工作区域       | Array[Tuple(UInt64,UInt8)]         | 同上                                                         |
| 7    | activity_location   | 活动区域       | Array[Tuple(UInt64,UInt8)]         | 同上                                                         |
| 8    | move_track          | 活动轨迹       | Array[Tuple(UInt64,UInt8)]         | 活动频率较高的轨迹点位集合                                   |
| 9    | day_activity        | 每日活动频率   | Array[Array[Tuple(UInt64,Uint32)]] | 统计过去一周每天每两小时的出现次数 [0,0,1,2,……,6,19]         |
| 10   | traffic_tools       | 交通工具       | Array[Tuple(Uint8,float64)]        | 1：步行 2：二轮车 3：三轮车 4：汽车 5：公共交通 [(1,33.20)(2,66.80)] |
| 11   | date                | 生成日期       | Date                               | 2023-12-16                                                   |

### 画像聚合表-人脸聚类

| No   | 字段名              | 字段说明       | 类型                              | 备注                                                         |
| ---- | ------------------- | -------------- | --------------------------------- | ------------------------------------------------------------ |
| 1    | group               | 聚类分组编号   | UInt64                            | 从人员聚类分组表获取的组号                                   |
| 3    | personnel_id_number | 人员身份证号   | String                            |                                                              |
| 2    | occupation          | 人员职业       | UInt8                             | 0:啥也不是 1:上班族 2：服务人员/保卫人员                     |
|      | occupation_prob     | 人员职业可信度 | Float64                           |                                                              |
| 3    | home_locations      | 住宅区域       | Array[Tuple(UInt64,UInt8)]        | [(3702014291461802,8)] 代表该点位出现时间                    |
| 4    | work_locations      | 工作区域       | Array[Tuple(UInt64,UInt8)]        | 同上                                                         |
| 5    | activity_location   | 活动区域       | Array[Tuple(UInt64,UInt8)]        | 同上                                                         |
| 6    | move_track          | 活动轨迹       | Array[Tuple(UInt64,UInt8)]        | 活动频率较高的轨迹点位集合                                   |
| 7    | day_activity        | 平均活动频率   | Array[Array[Tuple(UInt8,UInt64)]] | 统计过去一周每天每两小时的出现次数 [[2,0][4,3][5,10]……]……[[2,0],[4,0],[5,2]……] |
| 8    | traffic_tools       | 交通工具       | Array[Tuple(Uint8,float64)]       | 1：步行 2：二轮车 3：三轮车 4：汽车 5：公共交通 [(1,33.20)(2,66.80)] |
| 9    | update_time         | 更新时间       | UInt32                            | 时间戳                                                       |



### 驾乘聚类

| No   | 字段名              | 字段说明     | 类型                              | 备注                                                         |
| ---- | ------------------- | ------------ | --------------------------------- | ------------------------------------------------------------ |
| 1    | group               | 聚类分组编号 | UInt64                            | 从人员聚类分组表获取的组号                                   |
| 3    | personnel_id_number | 人员身份证号 | String                            |                                                              |
| 2    | occupation          | 人员职业     | UInt8                             | 0:啥也不是 1：网约车司机 2：出租车司机                       |
| 3    | home_locations      | 住宅区域     | Array[Tuple(UInt64,UInt8)]        | [(3702014291461802,8)] 代表该点位出现时间                    |
| 4    | work_locations      | 工作区域     | Array[Tuple(UInt64,UInt8)]        | 同上                                                         |
| 5    | activity_location   | 活动区域     | Array[Tuple(UInt64,UInt8)]        | 同上                                                         |
| 6    | move_track          | 活动轨迹     | Array[Tuple(UInt64,UInt8)]        | 活动频率较高的轨迹点位集合                                   |
| 7    | day_activity        | 平均活动频率 | Array[Array[Tuple(UInt8,UInt64)]] | 统计过去一周每天每两小时的出现次数 [[2,0][4,3][5,10]……]……[[2,0],[4,0],[5,2]……] |
| 8    | traffic_tools       | 交通工具     | Array[Tuple(Uint8,float64)]       | 1：步行 2：二轮车 3：三轮车 4：汽车 5：公共交通 [(1,33.20)(2,66.80)] |
| 9    | update_time         | 更新时间     | UInt32                            | 时间戳                                                       |



### 需求分析：

规则判断内容

统计时间：近三个月（90日），每个月5、10、15、20、25、30日更新数据（可在配置文件中修改）

活动频率：指的是一小时内在抓拍的次数

#### 人员画像：

（1）上班族：周一到周五平均每天活动时间（精确到时）存在顶点，且顶点间相距时间不低于3小时

思路：用聚类人员分组表，把过去三个月周一到周五的每天时间段出现次数相加，找到两个时间间隔大于3小时的极值顶点，这两个极值点占总数的1/2以上

判定依据：工作日活动存在两个固定的时间段（这两个时间段大于3小时-工作时间）和点位范围

（2）网约车司机：周一到周五、周六周天平均每天活动的时间很密集，且点位分散，且抓拍的图像中包含汽车数据，且80%的车辆类型不是出租车

思路：用驾乘人员分组表，过滤掉出租车，把过去三个月平均每天的活动频率大于一个最小阈值，且每小时活动频率求方差来衡量活动的稳定性

判定依据：每个小时都有频繁的活动，且这种特征过去90天都是稳定的

（3）出租车司机：周一到周五、周六周天平均每天活动的时间很密集，且点位分散，且抓拍的图像中包含汽车数据，且80%的车辆类型是出租车

思路：用驾乘人员分组表，筛选出租车，把过去三个月平均每天的活动频率大于一个最小阈值，不再求方差，只要每天出现即判定为出租车司机

判定依据：同网约车，研判对象只有出租车

#### 场所信息：

（1.1）住宅区域：周一到周五、周六周日对应平均每个点位每天活动时间频率较高的点位若是存在重叠，且对应活动的时间存在相邻情况（需排除频率最低的点位）（反序），则属于住宅区域点位

（1.2）工作区域：周一到周五对应平均每个点位每天活动时间频率较高的点位若是存在重叠，且对应活动的时间存在相邻情况（需排除频率最低的点位）（正序），则属于工作区域

（1.3）活跃区域：周一到周五、周六周日对应平均每个点位每天活动时间频率中高的点位。注：若点位数少于10个，则不判断活动时间相邻信息，频率较高为前50%，中高为30%-80%；若点位超过10个点，则频率较高为前30%，频率中高为30%-60%，频率较低为后30%。

思路：用聚类分组表,过去三个月每天出现频率最高的时间段内存在的点位存在重叠，且重叠次数>80，判定为住宅或者工作区域，根据时间（点位出现的时间是最早和最晚）区分住宅，根据点位距离区分工作区域和活动区域

#### 每日活动频率：

查数据库即可

#### 交通工具：

需统计并显示相邻的时间抓拍点位进行判断交通工具信息及频率 

● 统计的相邻两点位（仅计算超过500米）之间的时间不超过0.5小时，连续统计时段每个交通工具均记录一次 

● 两点之间抓拍时间及点位统计速度，若是两个抓拍都为行人/纯人脸，则速度大于10km/h且小于40km/h，则该交通工具判定为公共交通工具 

● 两点之间抓拍时间及点位统计速度，若是两个抓拍都为行人/纯人脸，则速度小于10km/h，则统计为步行； 

● 若是抓拍数据为单一点位，且父目标或本目标为行人则统计为步行； 

● 若是抓拍数据父目标为二轮车，则记为二轮车； 

● 若是抓拍数据父目标为三轮车，则记为三轮车；

