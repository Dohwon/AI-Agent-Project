# 순서

- 환경: Window + VS Code

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

- 환경변수 파일 만들기:

```
copy .env.template .env
```

* .env 열어서 (`notepad .env`) **API 키**와 **모델명** 세팅:

```
A 모델 -> 원본 모델 (inference_prompt.txt가 쓰일 모델)
MODEL_A_BASE_URL=https://api.openai.com (이후 주소는 py에서 생성하므로 여기까지)
MODEL_A_API_KEY=sk-... -> 본인(회사) 모델 API key
MODEL_A_MODEL=gpt-4o-mini -> openai 제공하는 모델명

B 모델 -> 비교할 모델
이하 동일

Judge -> Open AI 추천(JSON 보장때문에) 
이하 동일

MAX_CONCURRENCY -> 동시 요청 조절 (과하면 rate limit)
TIMEOUT -> 각 HTTP 요청 타임아웃 (느린 모델, 네트워크면 늘리기)
```

- 실행: `python judge_runner.py --tests sample_tests.csv --sys-prompt inference_prompt.txt --judge-prompt judge_prompt.txt --out results`

- 결과
	- `results.jsonl`: 각 테스트 케이스별 raw 결과 + judge JSON
	- `results.csv`: 요약(승자/신뢰도/케이스판정 등) 컬럼만 평탄화

- 결과 열 해석
	- `winner`: "A"|"B"|"tie"
	- `confidence`: 1~5
	- `case_winner`: 케이스 분류 기준 승자
	- `model_a_case` / `model_b_case`: Judge가 판독한 각 모델의 케이스
	- `response_a` / `response_b`: 원 응답(디버깅 편리)



## (25-11-04) 
- 현재는 Case1의 분류만 평가하고 있어서 Case 2(function/available function), Case 3의 확장이 필요 코드는 짰는데 테스트 귀찮아서 나중에 ㅌㅌ 