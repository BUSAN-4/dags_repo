-- ================================================
-- MariaDB 초기화 스키마
-- 데이터베이스: car_db
-- ================================================

CREATE DATABASE IF NOT EXISTS car_db;
USE car_db;

-- 1. 사용자-차량 정보 (uservehicle)
CREATE TABLE IF NOT EXISTS `uservehicle` (
  `car_id` varchar(255) NOT NULL COMMENT '차량 고유 ID',
  `age` int(11) DEFAULT NULL COMMENT '운전자 나이',
  `user_sex` enum('남','여') DEFAULT NULL COMMENT '운전자 성별',
  `user_location` varchar(255) DEFAULT NULL COMMENT '운전자 주소',
  `user_car_class` varchar(255) DEFAULT NULL COMMENT '차량 용도',
  `user_car_brand` varchar(255) DEFAULT NULL COMMENT '차량 브랜드',
  `user_car_year` int(11) DEFAULT NULL COMMENT '차량 연식',
  `user_car_model` varchar(255) DEFAULT NULL COMMENT '차량 차종',
  `user_car_weight` int(11) DEFAULT NULL COMMENT '차량 중량',
  `user_car_displace` int(11) DEFAULT NULL COMMENT '차량 배기량',
  `user_car_efficiency` varchar(255) DEFAULT NULL COMMENT '차량 연비',
  `updated_at` datetime DEFAULT NULL COMMENT '차량 정보 변경일',
  PRIMARY KEY (`car_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- 2. 운행 세션 (driving_session)
CREATE TABLE IF NOT EXISTS `driving_session` (
  `session_id` varchar(255) NOT NULL COMMENT '운행 세션 고유 ID',
  `car_id` varchar(255) DEFAULT NULL COMMENT '차량 고유 ID (FK)',
  `start_time` datetime DEFAULT NULL COMMENT '주행 시작 시간',
  `end_time` datetime DEFAULT NULL COMMENT '주행 종료 시간',
  `created_at` datetime DEFAULT NULL COMMENT '데이터 생성일',
  `updated_at` datetime DEFAULT NULL COMMENT '데이터 변경일',
  PRIMARY KEY (`session_id`),
  KEY `car_id` (`car_id`),
  CONSTRAINT `driving_session_ibfk_1` FOREIGN KEY (`car_id`) REFERENCES `uservehicle` (`car_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- 3. 운행 상세 정보 (driving_session_info)
CREATE TABLE IF NOT EXISTS `driving_session_info` (
  `info_id` char(36) NOT NULL DEFAULT uuid() COMMENT '운행기록 상세 정보ID(PK)',
  `session_id` varchar(255) DEFAULT NULL COMMENT '운행 세션 ID (FK)',
  `app_lat` double DEFAULT NULL COMMENT '현재 위도',
  `app_lon` double DEFAULT NULL COMMENT '현재 경도',
  `app_prev_lat` double DEFAULT NULL COMMENT '이전 위도',
  `app_prev_lon` double DEFAULT NULL COMMENT '이전 경도',
  `voltage` tinyint(4) DEFAULT NULL COMMENT '배터리 전압',
  `d_door` tinyint(4) DEFAULT NULL COMMENT '운전석 도어',
  `p_door` tinyint(4) DEFAULT NULL COMMENT '조수석 도어',
  `rd_door` tinyint(4) DEFAULT NULL COMMENT '운전석 후석 도어',
  `rp_door` tinyint(4) DEFAULT NULL COMMENT '조수석 후석 도어',
  `t_door` tinyint(4) DEFAULT NULL COMMENT '트렁크 도어',
  `engine_status` tinyint(4) DEFAULT NULL COMMENT '시동 상태',
  `r_engine_status` tinyint(4) DEFAULT NULL COMMENT '원격 시동',
  `stt_alert` tinyint(4) DEFAULT NULL COMMENT '경계 상태',
  `el_status` tinyint(4) DEFAULT NULL COMMENT '비상등',
  `detect_shock` tinyint(4) DEFAULT NULL COMMENT '충격 감지',
  `remain_remote` tinyint(4) DEFAULT NULL COMMENT '원격 시동 유지',
  `autodoor_use` tinyint(4) DEFAULT NULL COMMENT 'Auto door',
  `silence_mode` tinyint(4) DEFAULT NULL COMMENT '무음 모드',
  `low_voltage_alert` tinyint(4) DEFAULT NULL COMMENT '저전압 경고',
  `low_voltage_engine` tinyint(4) DEFAULT NULL COMMENT '저전압 시동',
  `temperature` tinyint(4) DEFAULT NULL COMMENT '내부 온도',
  `app_travel` tinyint(1) DEFAULT NULL COMMENT '운행 여부',
  `app_avg_speed` float DEFAULT NULL COMMENT '평균 속도',
  `app_accel` float DEFAULT NULL COMMENT '가속도',
  `app_gradient` float DEFAULT NULL COMMENT '차량 기울기',
  `app_rapid_acc` int(11) DEFAULT NULL COMMENT '급가속',
  `app_rapid_deacc` int(11) DEFAULT NULL COMMENT '급감속',
  `speed` float DEFAULT NULL COMMENT '통행 속도',
  `createdDate` datetime DEFAULT NULL COMMENT '교통 데이터 시간',
  `app_weather_status` varchar(255) DEFAULT NULL COMMENT '기상 상태',
  `app_precipitation` float DEFAULT NULL COMMENT '강수량',
  `dt` datetime DEFAULT NULL COMMENT '데이터등록시간',
  `roadname` varchar(50) DEFAULT NULL,
  `treveltime` double DEFAULT NULL,
  `Hour` int(11) DEFAULT NULL,
  PRIMARY KEY (`info_id`),
  KEY `session_id` (`session_id`),
  CONSTRAINT `driving_session_info_ibfk_1` FOREIGN KEY (`session_id`) REFERENCES `driving_session` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- 4. 차량 외부 이미지 (vehicle_exterior_image)
CREATE TABLE IF NOT EXISTS `vehicle_exterior_image` (
  `image_id` varchar(64) NOT NULL,
  `session_id` varchar(255) NOT NULL, -- session_id 타입 일치 (varchar(64) -> varchar(255))
  `captured_lat` double DEFAULT NULL,
  `captured_lon` double DEFAULT NULL,
  `captured_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `image_base64` longtext DEFAULT NULL COMMENT 'Base64 인코딩 이미지 데이터',
  `processed` tinyint(1) DEFAULT 0 COMMENT 'OCR 처리 여부 (0: 미처리, 1: 처리완료)',
  `plate_number` varchar(20) DEFAULT NULL COMMENT 'OCR 인식 번호판',
  `confidence` double DEFAULT NULL COMMENT 'OCR 인식 신뢰도',
  PRIMARY KEY (`image_id`),
  KEY `idx_vei_session` (`session_id`,`captured_at`),
  CONSTRAINT `fk_vei_session` FOREIGN KEY (`session_id`) REFERENCES `driving_session` (`session_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='차량 외부 카메라 수집 이미지';

-- 5. 졸음 운전 감지 (drowsy_drive)
CREATE TABLE IF NOT EXISTS `drowsy_drive` (
  `drowsy_id` varchar(64) NOT NULL,
  `session_id` varchar(255) NOT NULL, -- session_id 타입 일치
  `detected_lat` double DEFAULT NULL,
  `detected_lon` double DEFAULT NULL,
  `detected_at` datetime DEFAULT NULL,
  `duration_sec` int(11) DEFAULT NULL,
  `gaze_closure` int(11) DEFAULT NULL,
  `head_drop` int(11) DEFAULT NULL,
  `yawn_flag` int(11) DEFAULT NULL,
  `abnormal_flag` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`drowsy_id`),
  KEY `idx_drowsy_session` (`session_id`,`detected_at`),
  CONSTRAINT `fk_drowsy_session` FOREIGN KEY (`session_id`) REFERENCES `driving_session` (`session_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='운전자 졸음 탐지 이력';

-- 6. 체납 차량 정보 (arrears_info)
CREATE TABLE IF NOT EXISTS `arrears_info` (
  `car_plate_number` varchar(20) NOT NULL,
  `arrears_user_id` varchar(64) DEFAULT NULL,
  `total_arrears_amount` int(11) DEFAULT NULL,
  `arrears_period` varchar(50) DEFAULT NULL,
  `notice_sent` tinyint(1) DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`car_plate_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='체납 차량 관리 대장';

-- 7. 체납 차량 감지 (arrears_detection)
CREATE TABLE IF NOT EXISTS `arrears_detection` (
  `detection_id` varchar(64) NOT NULL,
  `image_id` varchar(64) NOT NULL,
  `car_plate_number` varchar(20) NOT NULL,
  `detection_success` tinyint(1) DEFAULT NULL,
  `detected_lat` double DEFAULT NULL,
  `detected_lon` double DEFAULT NULL,
  `detected_time` datetime DEFAULT NULL,
  PRIMARY KEY (`detection_id`),
  KEY `idx_arrears_image` (`image_id`,`detected_time`),
  KEY `idx_arrears_plate` (`car_plate_number`),
  CONSTRAINT `fk_arrears_image` FOREIGN KEY (`image_id`) REFERENCES `vehicle_exterior_image` (`image_id`),
  CONSTRAINT `fk_arrears_plate` FOREIGN KEY (`car_plate_number`) REFERENCES `arrears_info` (`car_plate_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='체납 차량 AI 탐지 결과';

-- 8. 실종자 정보 (missing_person_info)
CREATE TABLE IF NOT EXISTS `missing_person_info` (
  `missing_id` varchar(64) NOT NULL,
  `missing_name` varchar(100) DEFAULT NULL,
  `missing_age` int(11) DEFAULT NULL,
  `missing_identity` varchar(255) DEFAULT NULL,
  `registered_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `missing_location` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`missing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='실종자 등록 정보';

-- 9. 실종자 차량 감지 (missing_person_detection)
CREATE TABLE IF NOT EXISTS `missing_person_detection` (
  `detection_id` varchar(64) NOT NULL,
  `image_id` varchar(64) NOT NULL,
  `missing_id` varchar(64) NOT NULL,
  `detection_success` tinyint(1) DEFAULT NULL,
  `detected_lat` double DEFAULT NULL,
  `detected_lon` double DEFAULT NULL,
  `detected_time` datetime DEFAULT NULL,
  PRIMARY KEY (`detection_id`),
  KEY `idx_mpd_image` (`image_id`,`detected_time`),
  KEY `idx_mpd_missing` (`missing_id`),
  CONSTRAINT `fk_mpd_image` FOREIGN KEY (`image_id`) REFERENCES `vehicle_exterior_image` (`image_id`),
  CONSTRAINT `fk_mpd_missing` FOREIGN KEY (`missing_id`) REFERENCES `missing_person_info` (`missing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='실종자 AI 탐지 결과';

