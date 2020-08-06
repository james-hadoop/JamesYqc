CREATE TABLE `t_zcsd_user_log_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cid` varchar(50) DEFAULT NULL,
  `uid` varchar(50) DEFAULT NULL,
  `pid` varchar(50) DEFAULT NULL,
  `ts` varchar(50) DEFAULT NULL,
  `lat` varchar(50) DEFAULT NULL,
  `lon` varchar(50) DEFAULT NULL,
  `op` varchar(50) DEFAULT NULL,
  `cont` varchar(200) DEFAULT NULL,
  `ts_str` varchar(50) DEFAULT NULL,
  `lat_str` varchar(50) DEFAULT NULL,
  `lon_str` varchar(50) DEFAULT NULL,
  KEY `idx_id` (`id`),
  KEY `idx_cid` (`cid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COMMENT='政策速递用户行为表';


CREATE TABLE `t_zcsd_user_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `open_id` varchar(200) DEFAULT NULL,
  `nickname` varchar(500) DEFAULT NULL,
  `phone` varchar(200) DEFAULT NULL,
  `age` varchar(200) DEFAULT NULL,
  `sex` varchar(200) DEFAULT NULL,
  `duty` varchar(200) DEFAULT NULL,
  KEY `idx_id` (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=106 DEFAULT CHARSET=utf8mb4 COMMENT='政策速递用户信息表';


SELECT t_log.*,nickname,
             phone,
             age,
             sex,
             duty
   FROM
     (SELECT uid,
             cid,
             pid,
             ts,
             ts_str,
             lat_str,
             lon_str,
             op,
             count(op) AS cnt
      FROM zcsd.t_zcsd_user_log_detail
      GROUP BY uid,
               cid,
               pid,
               ts,
               ts_str,
               lat_str,
               lon_str,
               op) t_log
   LEFT JOIN
     (SELECT open_id,
             nickname,
             phone,
             age,
             sex,
             duty
      FROM zcsd.t_zcsd_user_info
      WHERE length(open_id)>1) t_u ON t_log.uid=t_u.open_id;