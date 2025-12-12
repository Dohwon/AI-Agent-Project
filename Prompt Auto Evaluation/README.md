- 251212 모델 순서 상관없이, 동일 모델 비교 가능하도록 개선

# 순서

## 환경

- Window + VS Code
- 폴더 전체 내 로컬에 다운로드
	- 폴더 내 파일 수정
		- **judge_prompt.txt**: 비교할 프롬프트
		- **inference_prompt.txt**: 원본 프롬프트
		- **sample_tests.csv**: 테스트할 문장(user_input)&기대결과 기록 
- VS Code 터미널에서 가상환경 생성:
   ```
   python -m venv .venv
   .\.venv\Scripts\activate
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```
	- (참고) VS Code 내 터미널은 ctrl+\`(백틱)
	- 혹시 powershall 실행 정책 때문에 `.\.venv\Scripts\activate`가 막힌다면 (**영구적용**):
	  `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force`
	  `#터미널 닫았다가 다시 열고:`
	  `cd {폴더}`
	  `.\.venv\Scripts\Activate.ps1`


## 환경변수 세팅 (모델 선정)

- 환경변수 파일 만들기:
```
copy .env.template .env
```
* .env 열어서 (`notepad .env`) **API 키**와 **모델명**, **비용**, **System prompt** 경로 세팅:
```
# 어떤 모델을 쓸지, 순서를 명시
MODELS=A,B,C       # A,B 만 써도 됨

# 공통: 비용(선택)
MODEL_A_INPUT_COST_PER_1K=0.15
MODEL_A_OUTPUT_COST_PER_1K=0.6
MODEL_B_INPUT_COST_PER_1K=0.0
MODEL_B_OUTPUT_COST_PER_1K=0.3
MODEL_C_INPUT_COST_PER_1K=
MODEL_C_OUTPUT_COST_PER_1K=

# A: OpenAI 호환 (GPT-4o-mini)
MODEL_A_VENDOR=openai
MODEL_A_BASE_URL=https://api.openai.com
MODEL_A_API_KEY=sk-...
MODEL_A_MODEL=gpt-4o-mini
MODEL_A_SYS_PROMPT=.\model_a_prompt.txt   # ← 파일 경로

# B: Gemini
MODEL_B_VENDOR=gemini
MODEL_B_BASE_URL=https://generativelanguage.googleapis.com
MODEL_B_API_KEY=AIza...
MODEL_B_MODEL=gemini-2.5-flash
MODEL_B_SYS_PROMPT=.\model_b_prompt.txt

# C: Upstage (Solar)
MODEL_C_VENDOR=upstage
MODEL_C_BASE_URL=https://api.upstage.ai/v1
MODEL_C_API_KEY=upstage-...
MODEL_C_MODEL=solar-pro-2
MODEL_C_SYS_PROMPT=.\model_c_prompt.txt

# Judge(그대로)
JUDGE_BASE_URL=https://api.openai.com
JUDGE_API_KEY=sk-...
JUDGE_MODEL=gpt-5.1

```
- 비교하려는 모델에 따라 A/B/C 모델을 설정해줘야 함.
- 3개 비교 시 라운드로빈(pairrwise 토너먼트)로 평가가 진행됨
- template은 OpenAI/Gemini/Upstage로 구성되어 있으니 적절히 선택 


- 실행: 3번째 모델 쓰고 싶으면 .env에 3번째 모델 추가! 
- 테스트 돌릴 파일 csv로 준비 + 파일명 다를경우 아래 `sample_test.csv` 수정
```powershell
python judge_runner.py --tests sample_tests.csv --judge-prompt judge_prompt.txt --out out
```

- 결과
	- `results_YYMMDD.jsonl`: 각 테스트 케이스별 raw 결과 + judge JSON
	- `results_YYMMDD.csv`: 요약(승자/신뢰도/케이스판정 등) 컬럼만 평탄화

- 결과 열 해석
	- `winner`: "A"|"B"|"tie"
	- `confidence`: 1~5
	- `case_winner`: 케이스 분류 기준 승자
	- `model_a_case` / `model_b_case`: Judge가 판독한 각 모델의 케이스
	- `response_a` / `response_b`: 원 응답(디버깅 편리)
