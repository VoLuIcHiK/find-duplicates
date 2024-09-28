<?php
//if (!getenv('FASTAPI_URL')) {
//    $_ENV['FASTAPI_URL'] = 'http://projectvoid.my.to:8054/check-video-duplicate';
//}

$post = [
    'link' => $_POST['link'],
    'id' => $_POST['id']
];
$curl = curl_init();

curl_setopt_array($curl, array(
    CURLOPT_URL => $_ENV['FASTAPI_URL'],
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_ENCODING => '',
    CURLOPT_MAXREDIRS => 10,
    CURLOPT_TIMEOUT => 0,
    CURLOPT_FOLLOWLOCATION => true,
    CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
    CURLOPT_CUSTOMREQUEST => 'POST',
    CURLOPT_POSTFIELDS =>'{
  "link": "'. $_POST['link'] . '" 
}',
    CURLOPT_HTTPHEADER => array(
        'Content-Type: application/json'
    ),
));

$response = curl_exec($curl);
curl_close($curl);
$result = json_decode($response, true);

if (!array_key_exists('duplicate_for', $result)) {
    $result['duplicate_for'] = NULL;
}

$conn = mysqli_connect('mysql', 'root', 'root', 'yappy_db');
$sql = "UPDATE video SET `related_video` = '{$result['duplicate_for']}' WHERE id = {$post['id']}";

if (!$conn->query($sql)) {
    print_r($conn->error);
}
echo $result['duplicate_for'];