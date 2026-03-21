-- MySQL dump 10.13  Distrib 8.0.43, for Win64 (x86_64)
--
-- Host: 127.0.0.1  
-- ------------------------------------------------------
-- Server version	8.4.7

CREATE DATABASE IF NOT EXISTS midterm_vn_firm_panel;
USE midterm_vn_firm_panel;

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dim_data_source`
--

DROP TABLE IF EXISTS `dim_data_source`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_data_source` (
  `source_id` smallint NOT NULL AUTO_INCREMENT,
  `source_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `source_type` enum('market','financial_statement','ownership','text_report','manual') COLLATE utf8mb4_unicode_ci NOT NULL,
  `provider` varchar(150) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `note` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`source_id`),
  UNIQUE KEY `source_name` (`source_name`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_data_source`
--

LOCK TABLES `dim_data_source` WRITE;
/*!40000 ALTER TABLE `dim_data_source` DISABLE KEYS */;
INSERT INTO `dim_data_source` VALUES (1,'FiinPro','ownership','FiinGroup','Ownership ratios (end-of-year snapshot)'),(2,'BCTC_Audited','financial_statement','Company/Exchange','Audited financial statements'),(3,'Vietstock','market','Vietstock','Market fields (price, shares, dividend, EPS)'),(4,'AnnualReport','text_report','Company','Annual report / disclosures for innovation & headcount');
/*!40000 ALTER TABLE `dim_data_source` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dim_exchange`
--

DROP TABLE IF EXISTS `dim_exchange`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_exchange` (
  `exchange_id` tinyint NOT NULL AUTO_INCREMENT,
  `exchange_code` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL,
  `exchange_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`exchange_id`),
  UNIQUE KEY `exchange_code` (`exchange_code`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_exchange`
--

LOCK TABLES `dim_exchange` WRITE;
/*!40000 ALTER TABLE `dim_exchange` DISABLE KEYS */;
INSERT INTO `dim_exchange` VALUES (1,'HOSE','Ho Chi Minh Stock Exchange'),(2,'HNX','Hanoi Stock Exchange');
/*!40000 ALTER TABLE `dim_exchange` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dim_firm`
--

DROP TABLE IF EXISTS `dim_firm`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_firm` (
  `firm_id` bigint NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `company_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `exchange_id` tinyint NOT NULL,
  `industry_l2_id` smallint DEFAULT NULL,
  `founded_year` smallint DEFAULT NULL,
  `listed_year` smallint DEFAULT NULL,
  `status` enum('active','delisted','inactive') COLLATE utf8mb4_unicode_ci DEFAULT 'active',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`),
  UNIQUE KEY `ticker` (`ticker`),
  KEY `fk_firm_exchange` (`exchange_id`),
  KEY `fk_firm_industry` (`industry_l2_id`),
  CONSTRAINT `fk_firm_exchange` FOREIGN KEY (`exchange_id`) REFERENCES `dim_exchange` (`exchange_id`),
  CONSTRAINT `fk_firm_industry` FOREIGN KEY (`industry_l2_id`) REFERENCES `dim_industry_l2` (`industry_l2_id`)
) ENGINE=InnoDB AUTO_INCREMENT=514 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_firm`
--

LOCK TABLES `dim_firm` WRITE;
/*!40000 ALTER TABLE `dim_firm` DISABLE KEYS */;
INSERT INTO `dim_firm` VALUES (1,'TEST','Test Corporation (for ETL/DB testing)',1,5,2010,2015,'active','2026-01-20 04:32:22','2026-01-20 04:32:22'),(2,'HPG','Hòa Phát',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(3,'VNM','VINAMILK',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(4,'GVR','Tập đoàn CN Cao su VN',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(5,'MSN','Tập đoàn Masan',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(6,'BSR','Lọc Hóa dầu Bình Sơn',1,4,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(7,'GEE','Thiết bị điện GELEX',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(8,'SAB','SABECO',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(9,'PLX','Petrolimex',1,4,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(10,'GEX','Tập đoàn Gelex',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(11,'DGC','Hóa chất Đức Giang',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(12,'KSV','Khoáng sản TKV',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(13,'PNJ','Vàng Phú Nhuận',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(14,'GMD','Gemadept',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(15,'HAG','Hoàng Anh Gia Lai',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(16,'SBT','Mía đường Thành Thành Công - Biên Hòa',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(17,'VGC','Tổng Công ty Viglacera',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(18,'HUT','Tasco- CTCP',2,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(19,'DCM','Đạm Cà Mau',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(20,'PVS','DVKT Dầu khí PTSC',2,4,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(21,'CII','Hạ tầng Kỹ thuật TP.HCM',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(22,'DPM','Tổng Công ty Phân bón và Hóa chất Dầu khí',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(23,'VCG','VINACONEX',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(24,'KDC','Tập đoàn KIDO',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(25,'PVD','Khoan Dầu khí PVDrilling',1,4,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(26,'BMP','Nhựa Bình Minh',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(27,'LGC','Đầu tư Cầu đường CII',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(28,'DHG','Dược Hậu Giang',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(29,'VHC','Thủy sản Vĩnh Hoàn',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(30,'VTP','Bưu chính Viettel',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(31,'HAH','Vận tải và Xếp dỡ Hải An',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(32,'NTP','Nhựa Tiền Phong',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(33,'HSG','Tập đoàn Hoa Sen',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(34,'CTR','Công trình Viettel',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(35,'DBC','Tập đoàn DABACO',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(36,'BAF','Nông nghiệp BAF Việt Nam',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(37,'PC1','Tập đoàn PC1',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(38,'CTD','Xây dựng Coteccons',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(39,'PVT','Vận tải Dầu khí PVTrans',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(40,'VSC','VICONSHIP',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(41,'PHR','Cao su Phước Hòa',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(42,'NKG','Thép Nam Kim',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(43,'ANV','Thủy sản Nam Việt',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(44,'VCF','VinaCafé Biên Hòa',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(45,'VCS','VICOSTONE',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(46,'IMP','IMEXPHARM',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(47,'BHN','HABECO',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(48,'HHV','Đầu tư Hạ tầng Giao thông Đèo Cả',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(49,'TMS','Transimex',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(50,'DHT','Dược phẩm Hà Tây',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(51,'HHS','Đầu tư DV Hoàng Huy',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(52,'HT1','VICEM Hà Tiên',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(53,'PAN','Tập đoàn PAN',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(54,'VIF','Lâm nghiệp Việt Nam',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(55,'SCG','Xây dựng SCG',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(56,'ACG','Gỗ An Cường',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(57,'PDN','Cảng Đồng Nai',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(58,'DBD','Dược - TB Y tế Bình Định',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(59,'TLG','Tập đoàn Thiên Long',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(60,'MSH','May Sông Hồng',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(61,'DPG','Tập đoàn Đạt Phương',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(62,'IPA','Tập đoàn Đầu tư I.P.A',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(63,'CSV','Hóa chất Cơ bản miền Nam',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(64,'STG','Kho Vận Miền Nam',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(65,'PTB','Công ty Cổ phần Phú Tài',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(66,'DPR','Cao su Đồng Phú',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(67,'HGM','Khoáng sản Hà Giang',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(68,'CDN','Cảng Đà Nẵng',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(69,'TCM','Dệt may Thành Công',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(70,'DHC','Đông Hải Bến Tre',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(71,'AAA','An Phát Bioplastics',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(72,'MCM','Giống bò sữa Mộc Châu',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(73,'ASM','Tập đoàn Sao Mai',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(74,'TRA','Traphaco',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(75,'DVP','ĐT và PT Cảng Đình Vũ',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(76,'NCT','DV Hàng hóa Nội Bài',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(77,'BFC','Phân bón Bình Điền',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(78,'DNP','DNP Holding',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(79,'TNG','Đầu tư và Thương mại TNG',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(80,'SVC','SAVICO',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(81,'DCL','Dược phẩm Cửu Long',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(82,'TDP','Công ty Thuận Đức',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(83,'FCN','FECON CORP',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(84,'SHI','Quốc tế Sơn Hà',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(85,'DRC','Cao su Đà Nẵng',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(86,'TV2','Tư vấn XD Điện 2',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(87,'FMC','Thực phẩm Sao Ta',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(88,'STK','Sợi Thế Kỷ',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(89,'LIX','Bột Giặt LIX',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(90,'VFG','Khử trùng Việt Nam',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(91,'TRC','Cao su Tây Ninh',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(92,'KSB','Khoáng sản Bình Dương',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(93,'LCG','LIZEN',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(94,'RAL','Bóng đèn Phích nước Rạng Đông',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(95,'PLC','Hóa dầu Petrolimex',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(96,'HHC','Bánh kẹo Hải Hà',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(97,'TNH','Tập đoàn Bệnh viện TNH',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(98,'NAF','Nafoods Group',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(99,'DMC','Dược phẩm DOMESCO',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(100,'SGN','Phục vụ mặt đất Sài Gòn',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(101,'IDI','Đầu tư và PT Đa Quốc Gia I.D.I',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(102,'LAS','Hóa chất Lâm Thao',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(103,'CTF','City Auto',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(104,'VOS','Vận tải Biển Việt Nam',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(105,'MVB','Mỏ Việt Bắc - TKV',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(106,'VGS','Ống thép Việt Đức',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(107,'EVG','Tập đoàn Everland',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(108,'HTG','Dệt may Hòa Thọ',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(109,'PAC','Pin Ắc quy Miền Nam',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(110,'SLS','Mía đường Sơn La',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(111,'LHC','XD Thủy lợi Lâm Đồng',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(112,'GIL','XNK Bình Thạnh',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(113,'PMC','Pharmedic',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(114,'FIT','Tập đoàn F.I.T',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(115,'NET','Bột giặt Net',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(116,'LBM','Khoáng sản Lâm Đồng',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(117,'APH','Tập đoàn An Phát Holdings',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(118,'BCF','Thực phẩm Bích Chi',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(119,'CDC','Chương Dương Corp',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(120,'THG','XD Tiền Giang',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(121,'ASG','Tập đoàn ASG',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(122,'CTI','Cường Thuận IDICO',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(123,'PVP','Vận tải Dầu khí Thái Bình Dương',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(124,'CSM','Cao su Miền Nam',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(125,'BBC','Bánh kẹo BIBICA',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(126,'SRC','Cao su Sao Vàng',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(127,'OPC','Dược phẩm OPC',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(128,'ACC','Đầu tư và XD Bình Dương ACC',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(129,'VTZ','Nhựa Việt Thành',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(130,'NSC','Tập đoàn Giống cây trồng Việt Nam',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(131,'CLC','Thuốc lá Cát Lợi',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(132,'NHH','Nhựa Hà Nội',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(133,'L18','LICOGI - 18',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(134,'QNP','Cảng Quy Nhơn',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(135,'OCH','Khách sạn và Dịch vụ OCH',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(136,'SMB','Bia Sài Gòn - Miền Trung',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(137,'NNC','Đá Núi Nhỏ',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(138,'INN','Bao bì và In Nông Nghiệp',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(139,'HAX','Ô tô Hàng Xanh',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(140,'DC4','Dicera Holdings',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(141,'TTF','Gỗ Trường Thành',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(142,'DP3','Dược Phẩm TW3',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(143,'VDP','Dược phẩm VIDIPHA',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(144,'HHP','HHP Global',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(145,'TFC','CTCP Trang',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(146,'VC7','BGI Group',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(147,'CLL','Cảng Cát Lái',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(148,'TCL','Tan Cang Logistics',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(149,'NHA','PT Nhà và Đô thị Nam HN',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(150,'L14','Licogi 14',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(151,'SMC','Đầu tư Thương mại SMC',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(152,'ILB','ICD Tân Cảng Long Bình',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(153,'S99','Sông Đà 9.09 (SCI)',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(154,'BCC','Xi măng Bỉm Sơn',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(155,'L40','Đầu tư và Xây dựng 40',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(156,'VIT','Viglacera Tiên Sơn',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(157,'CVT','CMC JSC',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(158,'NFC','Phân lân Ninh Bình',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(159,'VTO','VITACO',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(160,'PVC','Hóa chất và Dịch vụ Dầu khí',2,4,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(161,'DTL','Đại Thiên Lộc',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(162,'VIP','Vận tải Xăng dầu VIPCO',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(163,'DHA','Hóa An',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(164,'WCS','Bến xe Miền Tây',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(165,'HRC','Cao su Hòa Bình',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(166,'HVT','Hóa chất Việt trì',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(167,'TMB','Than Miền Bắc - Vinacomin',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(168,'PDV','Vận tải Phương Đông Việt',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(169,'HAP','Tập đoàn Hapaco',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(170,'JVC','Đầu tư Y tế - Dược phẩm Việt Nam',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(171,'ABT','Thủy sản Bến Tre',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(172,'LSS','Mía đường Lam Sơn',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(173,'DLG','Đức Long Gia Lai',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(174,'CLM','Xuất nhập khẩu Than - Vinacomin',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(175,'C69','Xây dựng 1369',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(176,'CSC','Tập đoàn COTANA',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(177,'SJE','Sông Đà 11',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(178,'SBG','Tập Đoàn Cơ Khí Công Nghệ Cao Siba',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(179,'VNC','VINACONTROL',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(180,'TPP','Nhựa Tân Phú VN',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(181,'GSP','Vận tải Sản phẩm Khí Quốc tế',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(182,'VAF','Phân lân Văn Điển',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(183,'CCC','Xây Dựng CDC',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(184,'CMX','CAMIMEX Group',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(185,'ACL','Thủy sản CL An Giang',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(186,'BTS','Xi măng Bút Sơn',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(187,'DXP','Cảng Đoạn Xá',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(188,'PVB','Bọc ống Dầu khí Việt Nam',2,4,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(189,'TLD','ĐT XD và PT Đô thị Thăng Long',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(190,'DL1','Tập đoàn Alpha 7',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(191,'TCD','Tập đoàn Xây dựng Tracodi',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(192,'SFI','Vận tải SAFI',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(193,'GMA','G-Automobile',2,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(194,'ADS','Dệt sợi DAMSAN',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(195,'DAT','ĐT Du lịch và PT Thủy sản',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(196,'SAF','Thực Phẩm SAFOCO',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(197,'TKU','Công nghiệp Tung Kuang',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(198,'CAP','Lâm nông sản Yên Bái',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(199,'HTI','PT Hạ tầng IDICO',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(200,'SHN','Đầu tư Tổng hợp Hà Nội',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(201,'CRC','Create Capital Việt Nam',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(202,'TNC','Cao su Thống Nhất',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(203,'TLH','Thép Tiến Lên',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(204,'CST','Than Cao Sơn - TKV',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(205,'TSC','Kỹ thuật NN Cần Thơ',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(206,'HVH','Đầu tư và Công nghệ HVC',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(207,'SVI','Bao bì Biên Hòa',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(208,'SGC','Bánh phồng tôm Sa Giang',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(209,'AME','Cơ điện Alphanam',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(210,'VPG','Đầu tư TMại XNK Việt Phát',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(211,'S55','Sông Đà 505',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(212,'TYA','Dây và Cáp điện Taya',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(213,'BKC','Khoáng sản Bắc Kạn',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(214,'VNT','Vận tải ngoại thương',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(215,'MCP','In và Bao bì Mỹ Châu',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(216,'ADP','Sơn Á Đông',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(217,'MHC','CTCP MHC',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(218,'VNE','Xây dựng điện Việt Nam',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(219,'PCT','Vận tải Biển Global Pacific',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(220,'HKT','Đầu tư QP Xanh',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(221,'PCH','Nhựa Picomat',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(222,'GDT','Gỗ Đức Thành',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(223,'RYG','Sản xuất và Đầu tư Hoàng Gia',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(224,'VNF','VINAFREIGHT',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(225,'SFG','Phân bón Miền Nam',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(226,'PHN','Pin Hà Nội',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(227,'VC2','Đầu tư và Xây dựng VINA2',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(228,'SJ1','Nông nghiệp Hùng Hậu',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(229,'HUB','Xây lắp Huế',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(230,'EVE','Everpia',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(231,'CIG','Xây dựng COMA 18',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(232,'TVD','Than Vàng Danh',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(233,'MST','Đầu tư MST',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(234,'VTV','Năng lượng và Môi trường VICEM',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(235,'TD6','Than Đèo Nai - Cọc Sáu - TKV',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(236,'HID','Halcom Vietnam',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(237,'SSC','Giống cây trồng Miền Nam',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(238,'TNT','Tập đoàn TNT',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(239,'PLP','SX và CN Nhựa Pha Lê',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(240,'LIG','Licogi 13',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(241,'HII','An Tiến Industries',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(242,'MAC','Tập đoàn Macstar',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(243,'MBG','Tập đoàn MBG',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(244,'SD9','Sông Đà 9',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(245,'TTL','TCT Thăng Long',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(246,'CTB','Bơm Hải Dương',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(247,'HSL','Thực phẩm Hồng Hà',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(248,'BCE','XD và GT Bình Dương',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(249,'C32','Đầu tư và Xây dựng 3-2',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(250,'C47','Xây dựng 47',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(251,'TMT','Ô tô TMT',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(252,'SAV','Savimex',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(253,'HLC','Than Hà Lầm',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(254,'TVT','May Việt Thắng',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(255,'HOM','Xi măng VICEM Hoàng Mai',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(256,'HTL','Ô tô Trường Long',1,8,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(257,'NAG','Tập đoàn Nagakawa',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(258,'VNL','Vinalink Logistics',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(259,'VC6','Visicons',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(260,'ICG','Xây dựng Sông Hồng',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(261,'HMC','Kim khí TP.HCM',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(262,'TNI','Tập đoàn Thành Nam',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(263,'DVM','Dược liệu Việt Nam',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(264,'NBC','Than Núi Béo',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(265,'TCO','TCO Holdings',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(266,'SDU','Đô thị Sông Đà',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(267,'CCR','Cảng Cam Ranh',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(268,'DQC','Tập đoàn Điện Quang',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(269,'MED','Dược Mediplantex',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(270,'TV4','Tư vấn XD Điện 4',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(271,'LAF','Chế biến Hàng XK Long An',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(272,'BAX','Công ty Thống Nhất',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(273,'VMS','Phát triển Hàng Hải',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(274,'HCD','SX và Thương mại HCD',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(275,'VSA','Đại lý Hàng hải VN',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(276,'SCI','SCI E&C',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(277,'VHL','Viglacera Hạ Long',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(278,'TPC','Nhựa Tân Đại Hưng',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(279,'ABR','Đầu tư Nhãn Hiệu Việt',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(280,'VE4','Xây dựng điện VNECO4',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(281,'VSI','Đầu tư và Xây dựng Cấp thoát nước',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(282,'AMV','Dược-TB Y tế Việt Mỹ',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(283,'SRF','SEAREFICO',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(284,'CLH','Xi măng La Hiên',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(285,'PHC','Xây dựng Phục Hưng Holdings',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(286,'BNA','Tập đoàn Đầu tư Bảo Ngọc',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(287,'QHD','Que hàn Việt Đức',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(288,'ABS','DV Nông nghiệp Bình Thuận',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(289,'VMD','Y Dược phẩm Vimedimex',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(290,'NHT','Sản xuất và Thương mại Nam Hoa',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(291,'DBT','Dược phẩm Bến Tre',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(292,'AAT','Tập đoàn Tiên Sơn Thanh Hóa',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(293,'TSB','Ắc quy Tia Sáng',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(294,'PMS','Cơ khí xăng dầu',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(295,'SC5','Xây dựng Số 5',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(296,'L10','LILAMA 10',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(297,'PTC','Đầu tư ICAPITAL',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(298,'VPS','Thuốc sát trùng Việt Nam',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(299,'NAP','Cảng Nghệ Tĩnh',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(300,'KHS','Thủy sản Kiên Hùng',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(301,'SD5','Sông Đà 5',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(302,'VCC','Vinaconex 25',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(303,'PJT','Vận tải thủy PETROLIMEX',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(304,'VGP','Cảng Rau Quả',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(305,'PDB','DIN Capital',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(306,'LDP','Dược Lâm Đồng - Ladophar',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(307,'MDG','Xây dựng Miền Đông',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(308,'CMS','Tập đoàn CMH Việt Nam',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(309,'X20','May mặc X20',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(310,'MDC','Than Mông Dương',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(311,'VID','Viễn Đông',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(312,'DHM','Khoáng sản Dương Hiếu',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(313,'HMH','Tập đoàn Hải Minh',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(314,'PCE','Phân bón và Hóa chất DK Miền Trung',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(315,'GLT','KT Điện Toàn Cầu',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(316,'BKG','Đầu tư BKG Việt Nam',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(317,'BMC','Khoáng sản Bình Định',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(318,'YBM','Khoáng sản CN Yên Bái',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(319,'THT','Than Hà Tu',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(320,'CJC','Cơ điện Miền Trung',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(321,'KTS','Đường Kon Tum',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(322,'GIC','ĐT Dịch vụ và PT Xanh',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(323,'CIA','DV Sân Bay Cam Ranh',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(324,'VMC','VIMECO',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(325,'HDA','Hãng sơn Đông Á',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(326,'NO1','Tâp đoàn 911',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(327,'VTB','Viettronics Tân Bình',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(328,'TET','May mặc Miền Bắc',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(329,'KMR','MIRAE',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(330,'FCM','Bê tông Phan Vũ Hà Nam',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(331,'SVD','Đầu tư & Thương mại Vũ Đăng',1,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(332,'HTV','Logistics Vicem',1,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(333,'PPP','PP.Pharco',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(334,'BRC','Cao su Bến Thành',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(335,'PPS','DVKT Điện lực Dầu khí',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(336,'HCC','Bê tông Hòa Cầm',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(337,'DTG','Dược phẩm Tipharco',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(338,'TDT','Đầu tư và Phát triển TDT',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(339,'VBC','Nhựa - Bao bì Vinh',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(340,'TV3','Tư vấn XD điện 3',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(341,'VC1','Xây dựng số 1',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(342,'TA9','Xây lắp Thành An 96',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(343,'V12','VINACONEX 12',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(344,'NST','Thuốc lá Ngân Sơn',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(345,'TOT','Vận tải Transimex',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(346,'SPM','S.P.M CORP',1,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(347,'CAN','Đồ hộp Hạ Long',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(348,'KDM','Tập đoàn GCL',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(349,'GMX','Gạch ngói Mỹ Xuân',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(350,'GMH','Minh Hưng Quảng Trị',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(351,'VHE','Dược liệu và Thực phẩm VN',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(352,'SHA','Sơn Hà Sài Gòn',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(353,'NAV','Tấm lợp và gỗ Nam Việt',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(354,'PSE','Phân bón và hóa chất dầu khí Đông Nam Bộ',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(355,'VCA','Thép VICASA - VNSTEEL',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(356,'PSW','Phân bón hóa chất dầu khí Tây Nam Bộ',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(357,'LM8','LILAMA 18',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(358,'VDL','Thực phẩm Lâm Đồng',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(359,'TCR','Gốm sứ TAICERA',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(360,'PMB','Phân bón và Hóa chất Dầu khí Miền Bắc',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(361,'DTT','Kỹ nghệ Đô Thành',1,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(362,'SDG','Sadico Cần Thơ',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(363,'VCM','BV Life',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(364,'MIC','Khoáng sản Quảng Nam',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(365,'HVX','Xi măng Vicem Hải Vân',1,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(366,'CTP','Hoà Bình Takara',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(367,'SRA','SARA Việt Nam',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(368,'DHP','Điện cơ Hải Phòng',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(369,'HAT','TM Bia Hà Nội',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(370,'SHE','PT Năng Lượng Sơn Hà',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(371,'TJC','Dịch vụ Vận tải và Thương mại',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(372,'KMT','Kim khí Miền Trung',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(373,'SPC','BV Thực vật Sài Gòn',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(374,'MEL','Thép Mê Lin',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(375,'CAG','Cảng An Giang',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(376,'THB','Bia Hà Nội - Thanh Hóa',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(377,'TTH','TM và DV Tiến Thành',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(378,'NSH','Nhôm Sông Hồng',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(379,'CTT','Chế tạo máy - Vinacomin',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(380,'GTA','Gỗ Thuận An',1,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(381,'CAR','Tập đoàn Giáo dục Trí Việt',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(382,'ITQ','Tập đoàn Thiên Quang',2,1,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(383,'PRC','Vận tải Portserco',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(384,'SDN','Sơn Đồng Nai',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(385,'DIH','PT Xây dựng Hội An',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(386,'GKM','GKM Holdings',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(387,'ARM','XNK Hàng không',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(388,'PSC','Vận tải Petrolimex SG',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(389,'MCF','Cơ khí và Lương thực Thực phẩm',2,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(390,'BBS','Bao bì Xi măng Bút Sơn',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(391,'LBE','Thương mại và Dịch vụ LVA',2,6,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(392,'VSM','Container Miền Trung',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(393,'MKV','Dược Thú Y Cai Lậy',2,9,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(394,'DC2','DIC Số 2',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(395,'AAM','Thủy sản Mekong',1,2,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(396,'CPC','Thuốc sát trùng Cần Thơ',2,3,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(397,'SDA','XKLĐ Sông Đà',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(398,'MCC','Gạch ngói cao cấp',2,7,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(399,'DS3','Quản lý Đường sông số 3',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(400,'VTH','Dây cáp điện Việt Thái',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45'),(401,'STP','CN Thương Mại Sông Đà',2,5,NULL,NULL,'active','2026-01-20 04:33:45','2026-01-20 04:33:45');
/*!40000 ALTER TABLE `dim_firm` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dim_industry_l2`
--

DROP TABLE IF EXISTS `dim_industry_l2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_industry_l2` (
  `industry_l2_id` smallint NOT NULL AUTO_INCREMENT,
  `industry_l2_name` varchar(150) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`industry_l2_id`),
  UNIQUE KEY `industry_l2_name` (`industry_l2_name`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_industry_l2`
--

LOCK TABLES `dim_industry_l2` WRITE;
/*!40000 ALTER TABLE `dim_industry_l2` DISABLE KEYS */;
INSERT INTO `dim_industry_l2` VALUES (4,'Dầu khí'),(5,'Hàng & Dịch vụ Công nghiệp'),(6,'Hàng cá nhân & Gia dụng'),(3,'Hóa chất'),(8,'Ô tô và phụ tùng'),(1,'Tài nguyên Cơ bản'),(2,'Thực phẩm và đồ uống'),(7,'Xây dựng và Vật liệu'),(9,'Y tế');
/*!40000 ALTER TABLE `dim_industry_l2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_cashflow_year`
--

DROP TABLE IF EXISTS `fact_cashflow_year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_cashflow_year` (
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `snapshot_id` bigint NOT NULL,
  `unit_scale` bigint NOT NULL DEFAULT '1',
  `currency_code` char(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'VND',
  `net_cfo` decimal(20,2) DEFAULT NULL,
  `capex` decimal(20,2) DEFAULT NULL,
  `net_cfi` decimal(20,2) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `fk_cf_snapshot` (`snapshot_id`),
  CONSTRAINT `fk_cf_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`),
  CONSTRAINT `fk_cf_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `fact_data_snapshot` (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_cashflow_year`
--

LOCK TABLES `fact_cashflow_year` WRITE;
/*!40000 ALTER TABLE `fact_cashflow_year` DISABLE KEYS */;
INSERT INTO `fact_cashflow_year` VALUES (1,2020,6,1000000000,'VND',12.00,6.00,-5.00,'2026-01-20 04:32:22'),(1,2021,7,1000000000,'VND',13.20,6.50,-5.80,'2026-01-20 04:32:22'),(1,2022,8,1000000000,'VND',14.50,7.20,-6.40,'2026-01-20 04:32:22'),(1,2023,9,1000000000,'VND',15.80,8.00,-7.10,'2026-01-20 04:32:22'),(1,2024,10,1000000000,'VND',18.20,9.50,-8.30,'2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_cashflow_year` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_data_snapshot`
--

DROP TABLE IF EXISTS `fact_data_snapshot`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_data_snapshot` (
  `snapshot_id` bigint NOT NULL AUTO_INCREMENT,
  `snapshot_date` date NOT NULL,
  `period_from` date DEFAULT NULL,
  `period_to` date DEFAULT NULL,
  `fiscal_year` smallint NOT NULL,
  `source_id` smallint NOT NULL,
  `version_tag` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `created_by` varchar(80) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`snapshot_id`),
  UNIQUE KEY `uq_snapshot` (`snapshot_date`,`fiscal_year`,`source_id`,`version_tag`),
  KEY `fk_snapshot_source` (`source_id`),
  CONSTRAINT `fk_snapshot_source` FOREIGN KEY (`source_id`) REFERENCES `dim_data_source` (`source_id`)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_data_snapshot`
--

LOCK TABLES `fact_data_snapshot` WRITE;
/*!40000 ALTER TABLE `fact_data_snapshot` DISABLE KEYS */;
INSERT INTO `fact_data_snapshot` VALUES (1,'2021-01-20',NULL,NULL,2020,1,'v1','seed','2026-01-20 04:32:22'),(2,'2022-01-20',NULL,NULL,2021,1,'v1','seed','2026-01-20 04:32:22'),(3,'2023-01-20',NULL,NULL,2022,1,'v1','seed','2026-01-20 04:32:22'),(4,'2024-01-20',NULL,NULL,2023,1,'v1','seed','2026-01-20 04:32:22'),(5,'2025-01-20',NULL,NULL,2024,1,'v1','seed','2026-01-20 04:32:22'),(6,'2021-03-31',NULL,NULL,2020,2,'v1','seed','2026-01-20 04:32:22'),(7,'2022-03-31',NULL,NULL,2021,2,'v1','seed','2026-01-20 04:32:22'),(8,'2023-03-31',NULL,NULL,2022,2,'v1','seed','2026-01-20 04:32:22'),(9,'2024-03-31',NULL,NULL,2023,2,'v1','seed','2026-01-20 04:32:22'),(10,'2025-03-31',NULL,NULL,2024,2,'v1','seed','2026-01-20 04:32:22'),(11,'2021-01-05',NULL,NULL,2020,3,'v1','seed','2026-01-20 04:32:22'),(12,'2022-01-05',NULL,NULL,2021,3,'v1','seed','2026-01-20 04:32:22'),(13,'2023-01-05',NULL,NULL,2022,3,'v1','seed','2026-01-20 04:32:22'),(14,'2024-01-05',NULL,NULL,2023,3,'v1','seed','2026-01-20 04:32:22'),(15,'2025-01-05',NULL,NULL,2024,3,'v1','seed','2026-01-20 04:32:22'),(16,'2021-04-15',NULL,NULL,2020,4,'v1','seed','2026-01-20 04:32:22'),(17,'2022-04-15',NULL,NULL,2021,4,'v1','seed','2026-01-20 04:32:22'),(18,'2023-04-15',NULL,NULL,2022,4,'v1','seed','2026-01-20 04:32:22'),(19,'2024-04-15',NULL,NULL,2023,4,'v1','seed','2026-01-20 04:32:22'),(20,'2025-04-15',NULL,NULL,2024,4,'v1','seed','2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_data_snapshot` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_financial_year`
--

DROP TABLE IF EXISTS `fact_financial_year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_financial_year` (
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `snapshot_id` bigint NOT NULL,
  `unit_scale` bigint NOT NULL DEFAULT '1',
  `currency_code` char(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'VND',
  `net_sales` decimal(20,2) DEFAULT NULL,
  `total_assets` decimal(20,2) DEFAULT NULL,
  `selling_expenses` decimal(20,2) DEFAULT NULL,
  `general_admin_expenses` decimal(20,2) DEFAULT NULL,
  `intangible_assets_net` decimal(20,2) DEFAULT NULL,
  `manufacturing_overhead` decimal(20,2) DEFAULT NULL,
  `net_operating_income` decimal(20,2) DEFAULT NULL,
  `raw_material_consumption` decimal(20,2) DEFAULT NULL,
  `merchandise_purchase_year` decimal(20,2) DEFAULT NULL,
  `wip_goods_purchase` decimal(20,2) DEFAULT NULL,
  `outside_manufacturing_expenses` decimal(20,2) DEFAULT NULL,
  `production_cost` decimal(20,2) DEFAULT NULL,
  `rnd_expenses` decimal(20,2) DEFAULT NULL,
  `net_income` decimal(20,2) DEFAULT NULL,
  `total_equity` decimal(20,2) DEFAULT NULL,
  `total_liabilities` decimal(20,2) DEFAULT NULL,
  `cash_and_equivalents` decimal(20,2) DEFAULT NULL,
  `long_term_debt` decimal(20,2) DEFAULT NULL,
  `current_assets` decimal(20,2) DEFAULT NULL,
  `current_liabilities` decimal(20,2) DEFAULT NULL,
  `growth_ratio` decimal(10,6) DEFAULT NULL,
  `inventory` decimal(20,2) DEFAULT NULL,
  `net_ppe` decimal(20,2) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `fk_fin_snapshot` (`snapshot_id`),
  CONSTRAINT `fk_fin_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`),
  CONSTRAINT `fk_fin_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `fact_data_snapshot` (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_financial_year`
--

LOCK TABLES `fact_financial_year` WRITE;
/*!40000 ALTER TABLE `fact_financial_year` DISABLE KEYS */;
INSERT INTO `fact_financial_year` VALUES (1,2020,6,1000000000,'VND',100.00,200.00,5.00,6.00,2.50,7.00,18.00,45.00,9.00,1.50,0.80,65.00,0.60,10.00,80.00,120.00,8.00,25.00,70.00,45.00,NULL,22.00,55.00,'2026-01-20 04:32:22'),(1,2021,7,1000000000,'VND',110.00,215.00,5.30,6.40,2.70,7.40,19.50,48.00,9.80,1.60,0.85,69.00,0.70,11.00,86.00,129.00,8.80,26.50,75.00,47.00,0.100000,23.50,58.00,'2026-01-20 04:32:22'),(1,2022,8,1000000000,'VND',120.00,230.00,5.60,6.80,3.00,7.90,21.00,51.00,10.50,1.70,0.90,73.00,0.85,12.00,92.00,138.00,9.50,28.00,80.00,49.50,0.090909,25.00,61.00,'2026-01-20 04:32:22'),(1,2023,9,1000000000,'VND',130.00,250.00,6.00,7.20,3.30,8.40,22.50,54.00,11.20,1.80,1.00,78.00,0.95,13.00,100.00,150.00,10.20,30.00,86.00,52.00,0.083333,26.80,65.00,'2026-01-20 04:32:22'),(1,2024,10,1000000000,'VND',145.00,275.00,6.60,7.80,3.70,9.20,25.00,60.00,12.50,2.00,1.15,86.00,1.20,15.00,112.00,163.00,12.00,33.00,95.00,56.00,0.115385,29.00,72.00,'2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_financial_year` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_firm_year_meta`
--

DROP TABLE IF EXISTS `fact_firm_year_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_firm_year_meta` (
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `snapshot_id` bigint NOT NULL,
  `employees_count` int DEFAULT NULL,
  `firm_age` smallint DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `fk_meta_snapshot` (`snapshot_id`),
  CONSTRAINT `fk_meta_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`),
  CONSTRAINT `fk_meta_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `fact_data_snapshot` (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_firm_year_meta`
--

LOCK TABLES `fact_firm_year_meta` WRITE;
/*!40000 ALTER TABLE `fact_firm_year_meta` DISABLE KEYS */;
INSERT INTO `fact_firm_year_meta` VALUES (1,2020,16,1000,11,'2026-01-20 04:32:22'),(1,2021,17,1050,12,'2026-01-20 04:32:22'),(1,2022,18,1100,13,'2026-01-20 04:32:22'),(1,2023,19,1150,14,'2026-01-20 04:32:22'),(1,2024,20,1200,15,'2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_firm_year_meta` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_innovation_year`
--

DROP TABLE IF EXISTS `fact_innovation_year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_innovation_year` (
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `snapshot_id` bigint NOT NULL,
  `product_innovation` tinyint DEFAULT NULL,
  `process_innovation` tinyint DEFAULT NULL,
  `evidence_source_id` smallint DEFAULT NULL,
  `evidence_note` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `fk_innov_snapshot` (`snapshot_id`),
  KEY `fk_innov_source` (`evidence_source_id`),
  CONSTRAINT `fk_innov_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`),
  CONSTRAINT `fk_innov_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `fact_data_snapshot` (`snapshot_id`),
  CONSTRAINT `fk_innov_source` FOREIGN KEY (`evidence_source_id`) REFERENCES `dim_data_source` (`source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_innovation_year`
--

LOCK TABLES `fact_innovation_year` WRITE;
/*!40000 ALTER TABLE `fact_innovation_year` DISABLE KEYS */;
INSERT INTO `fact_innovation_year` VALUES (1,2020,16,0,0,4,'Seed: no innovation reported','2026-01-20 04:32:22'),(1,2021,17,1,0,4,'Seed: launched new product line','2026-01-20 04:32:22'),(1,2022,18,0,1,4,'Seed: implemented new manufacturing process','2026-01-20 04:32:22'),(1,2023,19,1,1,4,'Seed: product + process innovation','2026-01-20 04:32:22'),(1,2024,20,1,0,4,'Seed: upgraded product portfolio','2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_innovation_year` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_market_year`
--

DROP TABLE IF EXISTS `fact_market_year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_market_year` (
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `snapshot_id` bigint NOT NULL,
  `shares_outstanding` bigint DEFAULT NULL,
  `price_reference` enum('close_year_end','avg_year','close_fiscal_end') COLLATE utf8mb4_unicode_ci DEFAULT 'close_year_end',
  `share_price` decimal(20,4) DEFAULT NULL,
  `market_value_equity` decimal(20,2) DEFAULT NULL,
  `dividend_cash_paid` decimal(20,2) DEFAULT NULL,
  `eps_basic` decimal(20,6) DEFAULT NULL,
  `currency_code` char(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'VND',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `fk_mkt_snapshot` (`snapshot_id`),
  CONSTRAINT `fk_mkt_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`),
  CONSTRAINT `fk_mkt_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `fact_data_snapshot` (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_market_year`
--

LOCK TABLES `fact_market_year` WRITE;
/*!40000 ALTER TABLE `fact_market_year` DISABLE KEYS */;
INSERT INTO `fact_market_year` VALUES (1,2020,11,1000000000,'close_year_end',20000.0000,20000000000000.00,500000000000.00,1000.000000,'VND','2026-01-20 04:32:22'),(1,2021,12,1000000000,'close_year_end',22000.0000,22000000000000.00,550000000000.00,1100.000000,'VND','2026-01-20 04:32:22'),(1,2022,13,1000000000,'close_year_end',21000.0000,21000000000000.00,600000000000.00,1200.000000,'VND','2026-01-20 04:32:22'),(1,2023,14,1000000000,'close_year_end',25000.0000,25000000000000.00,650000000000.00,1300.000000,'VND','2026-01-20 04:32:22'),(1,2024,15,1000000000,'close_year_end',28000.0000,28000000000000.00,700000000000.00,1500.000000,'VND','2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_market_year` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_ownership_year`
--

DROP TABLE IF EXISTS `fact_ownership_year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_ownership_year` (
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `snapshot_id` bigint NOT NULL,
  `managerial_inside_own` decimal(10,6) DEFAULT NULL,
  `state_own` decimal(10,6) DEFAULT NULL,
  `institutional_own` decimal(10,6) DEFAULT NULL,
  `foreign_own` decimal(10,6) DEFAULT NULL,
  `note` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `fk_own_snapshot` (`snapshot_id`),
  CONSTRAINT `fk_own_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`),
  CONSTRAINT `fk_own_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `fact_data_snapshot` (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_ownership_year`
--

LOCK TABLES `fact_ownership_year` WRITE;
/*!40000 ALTER TABLE `fact_ownership_year` DISABLE KEYS */;
INSERT INTO `fact_ownership_year` VALUES (1,2020,1,0.050000,0.000000,0.150000,0.200000,'seed','2026-01-20 04:32:22'),(1,2021,2,0.055000,0.000000,0.155000,0.210000,'seed','2026-01-20 04:32:22'),(1,2022,3,0.060000,0.000000,0.160000,0.220000,'seed','2026-01-20 04:32:22'),(1,2023,4,0.062000,0.000000,0.165000,0.230000,'seed','2026-01-20 04:32:22'),(1,2024,5,0.065000,0.000000,0.170000,0.240000,'seed','2026-01-20 04:32:22');
/*!40000 ALTER TABLE `fact_ownership_year` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fact_value_override_log`
--

DROP TABLE IF EXISTS `fact_value_override_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_value_override_log` (
  `override_id` bigint NOT NULL AUTO_INCREMENT,
  `firm_id` bigint NOT NULL,
  `fiscal_year` smallint NOT NULL,
  `table_name` varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL,
  `column_name` varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL,
  `old_value` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_value` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reason` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `changed_by` varchar(80) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `changed_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`override_id`),
  KEY `fk_override_firm` (`firm_id`),
  CONSTRAINT `fk_override_firm` FOREIGN KEY (`firm_id`) REFERENCES `dim_firm` (`firm_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_value_override_log`
--

LOCK TABLES `fact_value_override_log` WRITE;
/*!40000 ALTER TABLE `fact_value_override_log` DISABLE KEYS */;
/*!40000 ALTER TABLE `fact_value_override_log` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-01-22  1:31:50

-- APPEND MISSING INDUSTRIES

INSERT INTO dim_industry_l2 (industry_l2_id, industry_l2_name) 
VALUES 
(10, 'Vận tải'),
(11, 'Viễn thông')
ON DUPLICATE KEY UPDATE industry_l2_name = VALUES(industry_l2_name);

-- CLEAR SAMPLE DATA IN PREPARATION FOR ETL RUN

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE fact_cashflow_year;
TRUNCATE TABLE fact_financial_year;
TRUNCATE TABLE fact_market_year;
TRUNCATE TABLE fact_ownership_year;
TRUNCATE TABLE fact_innovation_year;
TRUNCATE TABLE fact_firm_year_meta;
TRUNCATE TABLE fact_data_snapshot;
TRUNCATE TABLE fact_value_override_log;
TRUNCATE TABLE dim_firm; 

-- GROUP SEED DATA --

SET FOREIGN_KEY_CHECKS = 0;

INSERT INTO dim_firm (ticker, company_name, exchange_id, industry_l2_id) 
VALUES 
('TEST', 'Test Corporation (For System Testing)', 1, 5),
('VTP', 'Viettel Post Joint Stock Corporation', 1, 10),
('PLC', 'Petrolimex Petrochemical Corporation - JSC', 2, 1),
('DHT', 'Ha Tay Pharmaceutical Joint Stock Company', 2, 9),
('ACG', 'An Cuong Wood - Working Joint Stock Company', 1, 1),
('RAL', 'Rang Dong Light Source and Vacuum Flask Joint Stock Company', 1, 5),
('SRC', 'Sao Vang Rubber Joint Stock Company', 1, 8),
('TMS', 'Transimex Corporation', 1, 10),
('CTR', 'Viettel Construction Joint Stock Corporation', 1, 11),
('SLS', 'Son La Sugar Joint Stock Company', 2, 2),
('PMC', 'Pharmedic Pharmaceutical Medicinal Joint Stock Company', 2, 9);

SET FOREIGN_KEY_CHECKS = 1;

-- CREATE MASTER VIEW (VW_FIRM_PANEL_LATEST)
CREATE OR REPLACE VIEW vw_firm_panel_latest AS
WITH LatestSnapshots AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) AS max_snap
    FROM fact_financial_year
    GROUP BY firm_id, fiscal_year
)
SELECT 
    f.ticker, 
    ls.fiscal_year,
    -- TABLES
    -- 1. FINANCIAL 
    fin.net_sales, fin.total_assets, fin.selling_expenses, fin.general_admin_expenses,
    fin.intangible_assets_net, fin.manufacturing_overhead, fin.net_operating_income,
    fin.raw_material_consumption, fin.merchandise_purchase_year, fin.wip_goods_purchase,
    fin.outside_manufacturing_expenses, fin.production_cost, fin.rnd_expenses,
    fin.net_income, fin.total_equity, fin.total_liabilities, fin.cash_and_equivalents,
    fin.long_term_debt, fin.current_assets, fin.current_liabilities, fin.growth_ratio,
    fin.inventory, fin.net_ppe,
    
    -- 2. OWNERSHIP 
    own.managerial_inside_own, own.state_own, own.institutional_own, own.foreign_own,
    
    -- 3. CASHFLOW 
    cf.net_cfo, cf.capex, cf.net_cfi,
    
    -- 4. MARKET 
    mkt.shares_outstanding, mkt.price_reference, mkt.share_price,
    mkt.market_value_equity, mkt.dividend_cash_paid, mkt.eps_basic,
    
    -- 5. INNOVATION 
    inv.product_innovation, inv.process_innovation,
    
    -- 6. META 
    meta.employees_count, meta.firm_age

FROM dim_firm f
JOIN LatestSnapshots ls ON f.firm_id = ls.firm_id
LEFT JOIN fact_financial_year fin 
    ON ls.firm_id = fin.firm_id AND ls.fiscal_year = fin.fiscal_year AND ls.max_snap = fin.snapshot_id
LEFT JOIN fact_ownership_year own 
    ON ls.firm_id = own.firm_id AND ls.fiscal_year = own.fiscal_year AND ls.max_snap = own.snapshot_id
LEFT JOIN fact_cashflow_year cf 
    ON ls.firm_id = cf.firm_id AND ls.fiscal_year = cf.fiscal_year AND ls.max_snap = cf.snapshot_id
LEFT JOIN fact_market_year mkt 
    ON ls.firm_id = mkt.firm_id AND ls.fiscal_year = mkt.fiscal_year AND ls.max_snap = mkt.snapshot_id
LEFT JOIN fact_innovation_year inv 
    ON ls.firm_id = inv.firm_id AND ls.fiscal_year = inv.fiscal_year AND ls.max_snap = inv.snapshot_id
LEFT JOIN fact_firm_year_meta meta 
    ON ls.firm_id = meta.firm_id AND ls.fiscal_year = meta.fiscal_year AND ls.max_snap = meta.snapshot_id;
