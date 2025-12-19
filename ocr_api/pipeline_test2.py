from ultralytics import YOLO
from paddleocr import PaddleOCR
import cv2
import os
import numpy as np
import re
import time
import json
from pathlib import Path
from datetime import datetime

# 1) YOLO 모델들
vehicle_detector = YOLO("yolov8m.pt")          # COCO 차량 탐지
plate_detector   = YOLO("plate_model/best.pt") # 번호판 탐지

# 2) PaddleOCR 추론용 경로 (export_model.py로 만든 폴더)
REC_MODEL_DIR = "car_plate_ number_model_inference_data"  # inference.pdmodel, .pdiparams, inference.yml 들어있는 폴더

# 3) PaddleOCR 인스턴스 (탐지는 YOLO가 하니까 det=False)
ocr = PaddleOCR(
    det=False,
    rec=True,
    rec_model_dir=REC_MODEL_DIR,
    use_angle_cls=False,
    lang='korean',
    use_gpu=False,  # CPU 버전이면 False로 설정
)

# 4) 폴더 생성
os.makedirs("crops/vehicle", exist_ok=True)
os.makedirs("crops/plate/validated", exist_ok=True)  # 검증된 번호판만 저장
os.makedirs("results", exist_ok=True)  # 시각화 결과 저장
os.makedirs("performance", exist_ok=True)  # 성능 지표 저장

# 5) OCR 신뢰도 임계값 설정
MIN_CONFIDENCE = 0.5  # 신뢰도 임계값 (조정 가능)


# ============================================================
# 번호판 형식 검증 함수 (선택사항)
# ============================================================
def is_valid_korean_plate(text):
    """한국 번호판 형식 검증 (예: 12가3456, 123가4567 등)"""
    # 공백 제거
    text = text.replace(' ', '').replace('-', '')
    # 숫자 + 한글 + 숫자 패턴
    pattern = r'^\d{2,3}[가-힣]\d{4}$'
    return bool(re.match(pattern, text))


# ============================================================
# 전체 파이프라인
# ============================================================
def run_pipeline(img_path):
    # 성능 측정 시작
    total_start = time.time()
    performance_metrics = {
        "image_path": img_path,
        "image_size": None,
        "timings": {},
        "detection": {
            "vehicles_detected": 0,
            "plates_detected": 0,
            "plates_validated": 0
        },
        "ocr_results": []
    }
    
    img = cv2.imread(img_path)
    if img is None:
        print(f"[오류] 이미지를 불러올 수 없습니다: {img_path}")
        return None
    
    h, w = img.shape[:2]
    performance_metrics["image_size"] = {"width": w, "height": h}

    print(f"\n[입력 이미지] {img_path} ({w}x{h})")
    
    # 차량 탐지 시간 측정
    vehicle_detect_start = time.time()
    vehicle_results = vehicle_detector(img, conf=0.25)
    performance_metrics["timings"]["vehicle_detection"] = time.time() - vehicle_detect_start

    vehicle_count = 0
    plate_count = 0
    ocr_results_list = []

    for i, vbox in enumerate(vehicle_results[0].boxes):
        cls = int(vbox.cls[0])
        # COCO: car=2, bus=5, truck=7 만 사용
        if cls not in [2, 5, 7]:
            continue

        # 1) 차량 crop
        vx1, vy1, vx2, vy2 = map(int, vbox.xyxy[0])
        vehicle_crop = img[vy1:vy2, vx1:vx2]
        vehicle_h, vehicle_w = vehicle_crop.shape[:2]
        
        # 차량 이미지는 일단 임시로 저장 (나중에 번호판 텍스트 알면 파일명 변경)
        vehicle_count += 1
        temp_vehicle_path = f"crops/vehicle/vehicle_{i}.jpg"
        cv2.imwrite(temp_vehicle_path, vehicle_crop)
        
        print(f"\n[차량 {vehicle_count}] {temp_vehicle_path}")

        # 2) 차량 내부에서 번호판 탐지 (시간 측정)
        plate_detect_start = time.time()
        plate_results = plate_detector(vehicle_crop, conf=0.10)
        plate_detect_time = time.time() - plate_detect_start
        
        if len(plate_results[0].boxes) == 0:
            print("  → 번호판이 탐지되지 않았습니다.")
            continue
        
        print(f"  → 탐지된 번호판: {len(plate_results[0].boxes)}개")
        
        # ============================================================
        # YOLO 신뢰도로 먼저 정렬 (OCR 전에 필터링)
        # ============================================================
        plate_boxes_sorted = []
        
        for j, pbox in enumerate(plate_results[0].boxes):
            px1, py1, px2, py2 = map(int, pbox.xyxy[0])
            yolo_conf = float(pbox.conf[0])  # YOLO 탐지 신뢰도
            
            plate_crop = vehicle_crop[py1:py2, px1:px2]
            plate_h, plate_w = plate_crop.shape[:2]
            
            # 최소 크기 검증
            if plate_h < 10 or plate_w < 10:
                print(f"  [번호판 {j+1}] 크기가 너무 작음: {plate_w}x{plate_h} (스킵)")
                continue
            
            plate_boxes_sorted.append({
                "index": j,
                "bbox": (px1, py1, px2, py2),
                "crop": plate_crop,
                "yolo_conf": yolo_conf
            })
        
        # YOLO 신뢰도로 정렬 (높은 순)
        plate_boxes_sorted.sort(key=lambda x: x["yolo_conf"], reverse=True)
        
        if len(plate_boxes_sorted) == 0:
            print("  → 유효한 번호판이 없습니다.")
            continue
        
        # ============================================================
        # 상위 1개만 OCR 실행 (속도 최적화)
        # ============================================================
        best_plate_info = plate_boxes_sorted[0]
        j = best_plate_info["index"]
        px1, py1, px2, py2 = best_plate_info["bbox"]
        plate_crop = best_plate_info["crop"]
        yolo_conf = best_plate_info["yolo_conf"]
        
        print(f"  [번호판 {j+1}] OCR 실행 중... (YOLO 신뢰도: {yolo_conf:.3f})")
        
        # OCR 실행 (시간 측정)
        ocr_start = time.time()
        try:
            raw = ocr.ocr(plate_crop, det=False, cls=False)
        except Exception as e:
            print(f"    → OCR 오류: {e}")
            continue
        ocr_time = time.time() - ocr_start
        
        texts = []
        scores = []
        if raw and raw[0]:
            for line in raw[0]:
                if isinstance(line, tuple) and len(line) >= 2:
                    text, score = line[0], line[1]
                    texts.append(text)
                    scores.append(score)
                elif isinstance(line, list) and len(line) >= 2:
                    if line[1] and isinstance(line[1], (list, tuple)) and len(line[1]) >= 2:
                        text = line[1][0]
                        score = line[1][1]
                        texts.append(text)
                        scores.append(score)
        
        final_text = texts[0] if texts else "인식 실패"
        final_score = scores[0] if scores else 0.0
        
        plate_count += 1
        
        print(f"  [번호판 {plate_count}] OCR 후보: {texts}")
        print(f"  최종 번호판: {final_text} (신뢰도: {final_score:.4f})")
        
        # 검증된 번호판만 저장
        is_valid = (final_text != "인식 실패" and final_score >= MIN_CONFIDENCE)
        
        if is_valid:
            safe_text = final_text.replace(' ', '_').replace('/', '_')
            
            # 차량 이미지 파일명에 번호판 텍스트 포함
            final_vehicle_path = f"crops/vehicle/vehicle_{i}_{safe_text}.jpg"
            if os.path.exists(temp_vehicle_path):
                os.rename(temp_vehicle_path, final_vehicle_path)
                print(f"  ✓ 차량 이미지 저장: {final_vehicle_path}")
            
            # 검증된 번호판 저장
            validated_path = f"crops/plate/validated/plate_{i}_{j}_{safe_text}.jpg"
            cv2.imwrite(validated_path, plate_crop)
            print(f"  ✓ 검증된 번호판 저장: {validated_path}")
            
            # ============================================================
            # 시각화 이미지 생성 (차량 + 번호판 텍스트 표시)
            # ============================================================
            vis_img = vehicle_crop.copy()
            
            # 번호판 영역에 박스 그리기 (초록색)
            cv2.rectangle(vis_img, (px1, py1), (px2, py2), (0, 255, 0), 2)
            
            # 번호판 텍스트 표시
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 0.8
            thickness = 2
            
            # 텍스트 내용
            text = f"{final_text} ({final_score:.2f})"
            (text_width, text_height), baseline = cv2.getTextSize(text, font, font_scale, thickness)
            
            # 텍스트 배경 (검은색 반투명)
            text_x = px1
            text_y = py1 - 10
            if text_y < text_height:
                text_y = py2 + text_height + 10  # 번호판 아래에 표시
            
            cv2.rectangle(vis_img, 
                         (text_x, text_y - text_height - 5), 
                         (text_x + text_width + 5, text_y + 5), 
                         (0, 0, 0), -1)
            
            # 텍스트 그리기 (초록색)
            cv2.putText(vis_img, text, 
                       (text_x + 2, text_y - 2), 
                       font, font_scale, (0, 255, 0), thickness)
            
            # 시각화 이미지 저장
            result_path = f"results/vehicle_{i}_{safe_text}_result.jpg"
            cv2.imwrite(result_path, vis_img)
            print(f"  ✓ 시각화 결과 저장: {result_path}")
        else:
            print(f"  ✗ 번호판 미저장 (신뢰도 부족 또는 인식 실패)")
        
        # 성능 지표에 OCR 결과 추가
        ocr_result = {
            "vehicle": vehicle_count,
            "plate": plate_count,
            "candidates": texts,
            "final": final_text,
            "score": final_score,
            "validated": is_valid,
            "yolo_confidence": yolo_conf,
            "plate_detect_time": plate_detect_time,
            "ocr_time": ocr_time
        }
        ocr_results_list.append(ocr_result)
        performance_metrics["ocr_results"].append(ocr_result)

    # 전체 처리 시간
    total_time = time.time() - total_start
    performance_metrics["timings"]["total"] = total_time
    performance_metrics["timings"]["plate_detection_total"] = sum(r["plate_detect_time"] for r in ocr_results_list)
    performance_metrics["timings"]["ocr_total"] = sum(r["ocr_time"] for r in ocr_results_list)
    
    # 탐지 통계
    performance_metrics["detection"]["vehicles_detected"] = vehicle_count
    performance_metrics["detection"]["plates_detected"] = plate_count
    performance_metrics["detection"]["plates_validated"] = sum(1 for item in ocr_results_list if item["validated"])
    
    # 평균 신뢰도 계산
    if ocr_results_list:
        performance_metrics["detection"]["avg_ocr_confidence"] = sum(r["score"] for r in ocr_results_list) / len(ocr_results_list)
        performance_metrics["detection"]["avg_yolo_confidence"] = sum(r["yolo_confidence"] for r in ocr_results_list) / len(ocr_results_list)
    else:
        performance_metrics["detection"]["avg_ocr_confidence"] = 0.0
        performance_metrics["detection"]["avg_yolo_confidence"] = 0.0

    print("\n===== 최종 요약 =====")
    print("탐지된 차량:", vehicle_count)
    print("탐지된 번호판:", plate_count)
    validated_count = sum(1 for item in ocr_results_list if item["validated"])
    print("검증된 번호판:", validated_count)

    print("\n--- OCR 전체 결과 ---")
    for item in ocr_results_list:
        print(f"[차량 {item['vehicle']} / 번호판 {item['plate']}]")
        print("  후보:", item["candidates"])
        print("  최종:", item["final"])
        print("  신뢰도:", f"{item['score']:.4f}")
        print("  검증:", "✓" if item["validated"] else "✗")
    
    # ============================================================
    # 성능 지표 출력 및 저장
    # ============================================================
    print("\n===== 성능 지표 =====")
    print(f"전체 처리 시간: {performance_metrics['timings']['total']:.2f}초")
    print(f"차량 탐지 시간: {performance_metrics['timings']['vehicle_detection']:.2f}초")
    print(f"번호판 탐지 시간: {performance_metrics['timings']['plate_detection_total']:.2f}초")
    print(f"OCR 처리 시간: {performance_metrics['timings']['ocr_total']:.2f}초")
    print(f"평균 OCR 신뢰도: {performance_metrics['detection']['avg_ocr_confidence']:.4f}")
    print(f"평균 YOLO 신뢰도: {performance_metrics['detection']['avg_yolo_confidence']:.4f}")
    
    # JSON 파일로 저장
    img_name = Path(img_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    performance_file = f"performance/{img_name}_{timestamp}.json"
    
    with open(performance_file, 'w', encoding='utf-8') as f:
        json.dump(performance_metrics, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ 성능 지표 저장: {performance_file}")
    
    return performance_metrics


# ============================================================
# 배치 처리 및 통합 성능 지표
# ============================================================
def process_batch_with_metrics(image_paths):
    """
    여러 이미지를 배치로 처리하고 통합 성능 지표 생성
    """
    batch_start = time.time()
    all_metrics = []
    
    print(f"\n{'='*60}")
    print(f"배치 처리 시작: {len(image_paths)}개 이미지")
    print(f"{'='*60}")
    
    for idx, img_path in enumerate(image_paths, 1):
        print(f"\n[{idx}/{len(image_paths)}] 처리 중...")
        metrics = run_pipeline(img_path)
        if metrics:
            all_metrics.append(metrics)
    
    batch_time = time.time() - batch_start
    
    # 통합 성능 지표 계산
    if all_metrics:
        total_metrics = {
            "batch_info": {
                "total_images": len(image_paths),
                "processed_images": len(all_metrics),
                "total_time": batch_time,
                "avg_time_per_image": batch_time / len(image_paths),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "aggregated_stats": {
                "total_vehicles": sum(m["detection"]["vehicles_detected"] for m in all_metrics),
                "total_plates": sum(m["detection"]["plates_detected"] for m in all_metrics),
                "total_validated": sum(m["detection"]["plates_validated"] for m in all_metrics),
                "avg_vehicle_detect_time": np.mean([m["timings"]["vehicle_detection"] for m in all_metrics]),
                "avg_plate_detect_time": np.mean([m["timings"]["plate_detection_total"] for m in all_metrics]),
                "avg_ocr_time": np.mean([m["timings"]["ocr_total"] for m in all_metrics]),
                "avg_total_time": np.mean([m["timings"]["total"] for m in all_metrics]),
                "avg_ocr_confidence": np.mean([m["detection"]["avg_ocr_confidence"] for m in all_metrics]),
                "avg_yolo_confidence": np.mean([m["detection"]["avg_yolo_confidence"] for m in all_metrics])
            },
            "individual_results": all_metrics
        }
        
        # 통합 성능 지표 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_performance_file = f"performance/batch_{timestamp}.json"
        
        with open(batch_performance_file, 'w', encoding='utf-8') as f:
            json.dump(total_metrics, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print("배치 처리 완료 - 통합 성능 지표")
        print(f"{'='*60}")
        print(f"총 이미지: {total_metrics['batch_info']['total_images']}개")
        print(f"처리 성공: {total_metrics['batch_info']['processed_images']}개")
        print(f"총 처리 시간: {total_metrics['batch_info']['total_time']:.2f}초")
        print(f"평균 처리 시간: {total_metrics['batch_info']['avg_time_per_image']:.2f}초/이미지")
        print(f"\n통합 통계:")
        print(f"  총 탐지된 차량: {total_metrics['aggregated_stats']['total_vehicles']}대")
        print(f"  총 탐지된 번호판: {total_metrics['aggregated_stats']['total_plates']}개")
        print(f"  총 검증된 번호판: {total_metrics['aggregated_stats']['total_validated']}개")
        print(f"  평균 차량 탐지 시간: {total_metrics['aggregated_stats']['avg_vehicle_detect_time']:.3f}초")
        print(f"  평균 번호판 탐지 시간: {total_metrics['aggregated_stats']['avg_plate_detect_time']:.3f}초")
        print(f"  평균 OCR 시간: {total_metrics['aggregated_stats']['avg_ocr_time']:.3f}초")
        print(f"  평균 OCR 신뢰도: {total_metrics['aggregated_stats']['avg_ocr_confidence']:.4f}")
        print(f"  평균 YOLO 신뢰도: {total_metrics['aggregated_stats']['avg_yolo_confidence']:.4f}")
        print(f"\n✓ 통합 성능 지표 저장: {batch_performance_file}")
        
        return total_metrics
    
    return None


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        img_path = sys.argv[1]
        run_pipeline(img_path)
    else:
        # 배치 처리 예시
        image_paths = [
            "../test_images/car_12.jpg",
            "../test_images/car_13.jpg",
            "../test_images/car_14.jpg",
            "../test_images/car_15.jpg"
        ]
        process_batch_with_metrics(image_paths)