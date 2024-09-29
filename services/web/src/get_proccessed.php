<?php
$post = [
    'link' => "http://{$_ENV['HOSTNAME']}:80/public/videos/{$_POST['video']}"
];
$ch = curl_init('https://webhook.site/8e55fc38-d728-472b-ac13-74c87805f723');
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_POST, 1);
curl_setopt($ch, CURLOPT_POSTFIELDS, $post);
curl_setopt($ch, CURLOPT_HEADER, false);
$html = curl_exec($ch);
curl_close($ch);

$conn = mysqli_connect('mysql', 'root', 'root', 'yappy_db');

echo json_encode(['link' => $post['link'], 'id' => mysqli_fetch_row($conn->query("SELECT MAX(id) FROM video"))]);
