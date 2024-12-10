#!/home/php8/bin/php -c/home/php8/lib/cli.ini
<?php
// 기본 몰아이디
const DEFAULT_MALL_ID = 'ectlocal';
// 최대 지원 row 수 (테이블당 해당 row 수를 초과하면 동작이 중단됩니다. 부하방지)
const LIMIT_ROWS = 10000;
// Diff CSV 파일 경로
const CSV_PATH = '/tmp/diffDataFinder.csv';
// 설정 파일 경로
const YAML_PATH = '';
// OpenAI API 키
const OPENAI_API_KEY = '';

// DatabaseModel 클래스는 데이터베이스와의 연결 및 백업, 비교 작업을 수행합니다.
class DatabaseModel {
    private $connection;

    // 생성자: 데이터베이스 연결을 설정합니다.
    public function __construct($db_config) {
        $connection_string = "host={$db_config['host']} port={$db_config['port']} dbname={$db_config['database']} user={$db_config['username']} password={$db_config['password']}";
        $this->connection = pg_connect($connection_string);
        if (!$this->connection) {
            throw new Exception("PostgreSQL 연결 중 오류가 발생했습니다.");
        }
    }

    // 지정된 스키마의 테이블을 백업합니다.
    public function backupTables($schema_name) {
        $this->setSchema($schema_name);
        return $this->executeBackup();
    }

    // 변경 후 테이블을 백업합니다.
    public function backupTablesAfterChange($schema_name) {
        $this->setSchema($schema_name);
        return $this->executeBackup(true);
    }

    // 스키마를 설정합니다.
    private function setSchema($schema_name) {
        $set_schema = pg_query($this->connection, "SET search_path TO " . pg_escape_string($schema_name));
        if (!$set_schema) {
            throw new Exception("스키마 설정 중 오류: " . pg_last_error($this->connection));
        }

        $current_schema_result = pg_query($this->connection, "SELECT current_schema();");
        if ($current_schema_result) {
            $current_schema = pg_fetch_result($current_schema_result, 0, 0);
            if (is_null($current_schema)) {
                throw new Exception("잘못된 몰 아이디 입니다");
            }
        } else {
            throw new Exception("현재 스키마를 가져오는 중 오류: " . pg_last_error($this->connection));
        }
    }

    // 테이블을 백업합니다. 변경 전/후에 따라 접미사를 다르게 설정합니다.
    private function executeBackup($isAfterChange = false) {
        $suffix = $isAfterChange ? '_after' : '_before';

        // 현재 스키마를 가져옵니다.
        $result = pg_query($this->connection, "SELECT current_schema();");
        $current_schema = pg_fetch_result($result, 0, 0);

        // 테이블 목록을 가져옵니다.
        $tables_result = pg_query($this->connection, "SELECT tablename FROM pg_tables WHERE schemaname = '{$current_schema}'");

        while ($table = pg_fetch_assoc($tables_result)) {
            $tablename = $table['tablename'];

            // 각 테이블의 행 수를 확인합니다.
            $count_result = pg_query($this->connection, "SELECT COUNT(*) FROM {$tablename}");
            $row_count = pg_fetch_result($count_result, 0, 0);

            if ($row_count > LIMIT_ROWS) {
                die("테이블 {$tablename}의 행 수가 10000개를 초과합니다. 백업이 중단되었습니다.\n");
            }

            // 임시 테이블 생성
            $backup_query = "CREATE TEMP TABLE {$tablename}{$suffix} AS TABLE {$tablename}";
            pg_query($this->connection, $backup_query);
        }
        return true;
    }

    // 테이블을 비교하여 차이점을 찾습니다.
    public function compareTables($schema_name) {
        $this->setSchema($schema_name);
        $query = "
            DO $$
            DECLARE
                r RECORD;
                current_schema TEXT;
                col RECORD;
                col_list TEXT;
                before_query TEXT;
                after_query TEXT;
                diff_count INT;
            BEGIN
                CREATE TEMP TABLE diff_data_finder (
                    log_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    log_message TEXT
                );

                SELECT current_schema INTO current_schema;
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema) LOOP
                    col_list := '';

                    FOR col IN (SELECT column_name, data_type FROM information_schema.columns WHERE table_name = r.tablename AND table_schema = current_schema) LOOP
                        IF col.data_type NOT IN ('json', 'jsonb') THEN
                            col_list := col_list || 'CAST(' || col.column_name || ' AS TEXT) AS ' || col.column_name || ', ';
                        END IF;
                    END LOOP;

                    col_list := LEFT(col_list, LENGTH(col_list) - 2);

                    before_query := 'SELECT * FROM (SELECT ' || col_list || ' FROM ' || r.tablename || '_before EXCEPT SELECT ' || col_list || ' FROM ' || r.tablename || '_after) AS diff_before';
                    after_query := 'SELECT * FROM (SELECT ' || col_list || ' FROM ' || r.tablename || '_after EXCEPT SELECT ' || col_list || ' FROM ' || r.tablename || '_before) AS diff_after';

                    RAISE NOTICE 'Comparing table: %', r.tablename;
                    INSERT INTO diff_data_finder (log_message) VALUES ('Comparing table: ' || r.tablename);

                    EXECUTE 'CREATE TEMP TABLE temp_diff_before AS ' || before_query;
                    EXECUTE 'CREATE TEMP TABLE temp_diff_after AS ' || after_query;

                    SELECT COUNT(*) INTO diff_count FROM temp_diff_before;
                    IF diff_count > 0 THEN
                        RAISE NOTICE 'Table % has changes in diff_before: %', r.tablename, diff_count;
                        INSERT INTO diff_data_finder (log_message) VALUES ('Table ' || r.tablename || ' has changes in diff_before: ' || diff_count);
                        FOR col IN (SELECT * FROM temp_diff_before) LOOP
                            RAISE NOTICE 'Diff Before: %', row_to_json(col);
                            INSERT INTO diff_data_finder (log_message) VALUES ('Diff Before: ' || row_to_json(col)::text);
                        END LOOP;
                    END IF;

                    SELECT COUNT(*) INTO diff_count FROM temp_diff_after;
                    IF diff_count > 0 THEN
                        RAISE NOTICE 'Table % has changes in diff_after: %', r.tablename, diff_count;
                        INSERT INTO diff_data_finder (log_message) VALUES ('Table ' || r.tablename || ' has changes in diff_after: ' || diff_count);
                        FOR col IN (SELECT * FROM temp_diff_after) LOOP
                            RAISE NOTICE 'Diff After: %', row_to_json(col);
                            INSERT INTO diff_data_finder (log_message) VALUES ('Diff After: ' || row_to_json(col)::text);
                        END LOOP;
                    END IF;

                    EXECUTE 'DROP TABLE IF EXISTS temp_diff_before';
                    EXECUTE 'DROP TABLE IF EXISTS temp_diff_after';
                    EXECUTE 'DROP TABLE IF EXISTS ' || r.tablename || '_before';
                    EXECUTE 'DROP TABLE IF EXISTS ' || r.tablename || '_after';
                END LOOP;
            END $$;
        ";
        return pg_query($this->connection, $query);
    }

    // 차이점을 CSV 파일로 저장합니다.
    public function saveDiffToCSV() {
        $save_to_csv = pg_query($this->connection, "
            DO $$
            BEGIN
                EXECUTE 'COPY (SELECT log_message FROM diff_data_finder ORDER BY log_time ASC) TO ''" . CSV_PATH . "'' CSV HEADER';
                EXECUTE 'DROP TABLE IF EXISTS diff_data_finder';
            END $$;
        ");
        if (!$save_to_csv) {
            throw new Exception("CSV 파일로 저장하는 중 오류: " . pg_last_error($this->connection));
        }
        pg_close($this->connection);
        return CSV_PATH;
    }
}

// DatabaseController 클래스는 모델을 사용하여 데이터베이스 작업을 처리합니다.
class DatabaseController {
    private $model;

    // 생성자: 모델을 초기화합니다.
    public function __construct($db_config) {
        $this->model = new DatabaseModel($db_config);
    }

    // 백업 및 비교 프로세스를 처리합니다.
    public function processBackupAndCompare($schema_name) {
        try {
            $this->model->backupTables($schema_name);
            echo "\n기존 데이터 백업이 완료되었습니다.\n";

            echo "확인하고자 하는 액션을 진행하신 후 Enter 키를 눌러주세요: ";
            fgets(STDIN);

            $this->model->backupTablesAfterChange($schema_name);
            echo "변경 후 데이터 레코딩이 완료되었습니다.\n";
            echo "변경 전/후 데이터 비교 중...\n";

            $this->model->compareTables($schema_name);
            echo "데이터 비교가 완료되었습니다.\n";

            $csvFile = $this->model->saveDiffToCSV();
            echo "\n변경 사항이 '$csvFile' 파일에 저장되었습니다.\n";
            echo "분석 결과를 확인하려면 Enter 키를 눌러주세요: ";
            fgets(STDIN);

            $results = $this->processDiffData($csvFile);
            echo "\n변경 사항 분석 결과:\n";
            echo print_r($results, true);

            echo "\nOpenAI 분석중...\n";
            $summary = $this->summarizeWithOpenAI($results);
            echo "\nOpenAI 분석 요약:\n";
            echo "------------------------\n";
            echo $summary;
            echo "\n------------------------\n";

        } catch (Exception $e) {
            echo $e->getMessage();
        }
    }

    // 차이 데이터를 처리합니다.
    private function processDiffData($filename) {
        return (new DiffDataFinder($filename))->getResult();
    }

    // OpenAI를 사용하여 요약합니다.
    private function summarizeWithOpenAI($results) {
        $api_key = OPENAI_API_KEY;
        if (!$api_key) {
            return "OpenAI API 키가 설정되지 않았습니다. const::OPENAI_API_KEY 를 설정해 주세요.\n";
        }

        $prompt = "
            ## 데이터 변경 사항 요약
            - 변경사항을 마크다운 형태로 출력하세요.
            
            ### New row 데이터
            - 각 테이블에 대해 추가된 row의 수를 다음과 같은 형식으로 출력하세요.
              - 예시:
                ```
                table_name
                - 2개의 새로운 row가 추가되었습니다.
                ```
            
            ### Change row 데이터
            - AS-IS 데이터와 TO-BE 데이터의 차이점을 다음과 같은 형식으로 출력하세요.
              - 예시:
                ```
                table_name.column_name
                - AS-IS: 1234567890
                - TO-BE: 9876543210
                ```
        " . json_encode($results, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://api.openai.com/v1/chat/completions');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/json',
            'Authorization: Bearer ' . $api_key
        ]);

        $data = [
            'model' => 'gpt-4o',
            'messages' => [
                [
                    'role' => 'system',
                    'content' => '당신은 데이터 변경사항을 분석하고 요약하는 전문가입니다.'
                ],
                [
                    'role' => 'user',
                    'content' => $prompt
                ]
            ],
            'temperature' => 0.7
        ];

        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));

        $response = curl_exec($ch);
        if (curl_errno($ch)) {
            return 'OpenAI API 호출 중 오류 발생: ' . curl_error($ch);
        }
        curl_close($ch);

        $response_data = json_decode($response, true);
        return $response_data['choices'][0]['message']['content'] ?? "응답 처리 중 오류가 발생했습니다.";
    }
}

// ConsoleView 클래스는 사용자와의 상호작용을 처리합니다.
class ConsoleView {
    // 메시지를 출력합니다.
    public function displayMessage($message) {
        echo $message . "\n";
    }

    // 사용자 입력을 요청합니다.
    public function prompt($message) {
        echo $message;
        return trim(fgets(STDIN));
    }

    // 배너를 표시합니다.
    public function displayBanner() {
        echo "\n";
        echo "██████╗ ██╗███████╗███████╗    ██████╗  █████╗ ████████╗ █████╗     ███████╗██╗███╗   ██╗██████╗ ███████╗██████╗ \n";
        echo "██╔══██╗██║██╔════╝██╔════╝    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██╔════╝██║████╗  ██║██╔══██╗██╔════╝██╔══██╗\n";
        echo "██║  ██║██║█████╗  █████╗      ██║  ██║███████║   ██║   ███████║    █████╗  ██║██╔██╗ ██║██║  ██║█████╗  ██████╔╝\n";
        echo "██║  ██║██║██╔══╝  ██╔══╝      ██║  ██║██╔══██║   ██║   ██╔══██║    ██╔══╝  ██║██║╚██╗██║██║  ██║██╔══╝  ██╔══██╗\n";
        echo "██████╔╝██║██║     ██║         ██████╔╝██║  ██║   ██║   ██║  ██║    ██║     ██║██║ ╚████║██████╔╝███████╗██║  ██║\n";
        echo "╚═════╝ ╚═╝╚═╝     ╚═╝         ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝    ╚═╝     ╚═╝╚═╝  ╚═══╝╚═════╝ ╚══════╝╚═╝  ╚═╝\n";
        echo "\n";
        echo "=================================================================================================\n";
        echo "                                Data Change Tracking Tool v1.0                                 \n";
        echo "=================================================================================================\n";
        echo "Developer: wjsong\n";
        echo "Warning!!!: 실몰 절대 사용 금지!!!\n";
        echo "\n";
        echo "[ 사용 방법 ]\n";
        echo "1. 몰아이디를 입력하면 현재 상태가 백업됩니다.\n";
        echo "2. 확인하고자 하는 액션을 수행하세요.\n";
        echo "3. 작업이 완료되면 Enter 키를 눌러주세요.\n";
        echo "4. 변경 사항이 CSV 파일로 저장되고 OpenAI를 통해 요약됩니다.\n";
        echo "\n";
        echo "시작하려면 아무 키나 눌러주세요...";
        fgets(STDIN);
        echo "\n";
    }
}

// DiffDataFinder 클래스는 CSV 파일에서 차이점을 분석합니다.
class DiffDataFinder {
    private $filename;
    private $notices;
    private $changedTables = [];
    private $aResult = [
        'changed_tables' => [],
        'new_rows' => [],
        'change_rows' => []
    ];

    // 생성자: 파일을 읽고 분석을 시작합니다.
    public function __construct($filename) {
        $this->filename = $filename;
        $this->notices = file($filename, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        $this->mainFunc();
    }

    // 주요 분석 기능을 수행합니다.
    private function mainFunc() {
        $currentTable = '';

        foreach ($this->notices as $notice) {
            if (strpos($notice, 'Table') !== false) {
                preg_match('/Table (\w+) has changes/', $notice, $matches);
                $currentTable = $matches[1];
            } elseif (strpos($notice, 'Diff Before:') !== false) {
                $cleanedNotice = $this->cleanNotice($notice, 'Diff Before: ');
                $this->changedTables[$currentTable]['diff_before'][] = json_decode($cleanedNotice, true);
            } elseif (strpos($notice, 'Diff After:') !== false) {
                $cleanedNotice = $this->cleanNotice($notice, 'Diff After: ');
                $this->changedTables[$currentTable]['diff_after'][] = json_decode($cleanedNotice, true);
            }
        }

        $this->findChanges();
    }

    // 공지 문자열을 정리합니다.
    private function cleanNotice($notice, $prefix) {
        $notice = trim($notice, '"');
        $notice = str_replace($prefix, '', $notice);
        return str_replace('""', '"', $notice);
    }

    // 변경 사항을 찾습니다.
    private function findChanges() {
        foreach ($this->changedTables as $table => $diffs) {
            $diffBefore = $diffs['diff_before'] ?? [];
            $diffAfter = $diffs['diff_after'] ?? [];

            if (count($diffBefore) === 0 && count($diffAfter) > 0) {
                $this->aResult['changed_tables'][] = $table;
                $this->aResult['new_rows'][$table][] = $diffAfter;
            } elseif (count($diffBefore) > 0 && count($diffAfter) > 0) {
                $this->aResult['changed_tables'][] = $table;
                $this->aResult['change_rows'][$table]['origin'] = $this->arrayRecursiveDiff($diffAfter, $diffBefore);
                $this->aResult['change_rows'][$table]['updated'] = $this->arrayRecursiveDiff($diffBefore, $diffAfter);
            }
        }
    }

    // 두 배열의 차이점을 재귀적으로 찾습니다.
    private function arrayRecursiveDiff($array1, $array2) {
        $result = [];
        foreach ($array2 as $key => $value) {
            if (array_key_exists($key, $array1)) {
                if (is_array($value)) {
                    $recursiveDiff = $this->arrayRecursiveDiff($array1[$key], $value);
                    if (count($recursiveDiff)) {
                        $result[$key] = $recursiveDiff;
                    }
                } elseif ($value != $array1[$key]) {
                    $result[$key] = $value;
                }
            } else {
                $result[$key] = $value;
            }
        }
        return $result;
    }

    // 분석 결과를 반환합니다.
    public function getResult() {
        return $this->aResult;
    }
}

// 메인 실행 부분
$view = new ConsoleView();
$view->displayBanner();

$yaml_file = YAML_PATH;
if (!function_exists('yaml_parse_file')) {
    die("YAML 확장이 설치되어 있지 않습니다. 설치 후 진행해 주세요.\n");
}

$config = yaml_parse_file($yaml_file);
if (!$config) {
    die("YAML 파일을 파싱하는 데 실패했습니다.\n");
}

$db_config = $config['default']['postgresql_mall_master'];

echo "몰아이디를 입력하세요 (default: " . DEFAULT_MALL_ID . "): ";
$input_schema_name = trim(fgets(STDIN));

if (empty($input_schema_name)) {
    $input_schema_name = DEFAULT_MALL_ID;
    echo "기본값 '" . DEFAULT_MALL_ID . "'이 사용됩니다.\n";
}

// 입력값 유효성 검사
if (!preg_match('/^(' . DEFAULT_MALL_ID . '|ectopsue)/', $input_schema_name)) {
    die("잘못된 스키마 이름입니다. '" . DEFAULT_MALL_ID . "' 또는 'ectopsue'로 시작해야 합니다.\n");
}

$schema_name = 'ec_' . $input_schema_name;
$controller = new DatabaseController($db_config);
$controller->processBackupAndCompare($schema_name);

?>
