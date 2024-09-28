<?php
define ('SITE_ROOT', realpath(dirname(__FILE__)));
$conn = mysqli_connect('mysql', 'root', 'root', 'yappy_db');

if($conn->connect_error) {
    die("Ошибка: ". $conn->connect_error);
}

$tmp_name = $_FILES["video"]["tmp_name"];
$name = str_contains(basename($_FILES["video"]["name"]), "'") ?
    str_replace("'", '', basename($_FILES["video"]["name"])) :
    basename($_FILES["video"]["name"]);

move_uploaded_file($tmp_name, SITE_ROOT."/public/videos/$name");
$sql = "INSERT INTO video (`id`, `file`, `status`, `proccessing_time`, `upload_date`, `related_video`) VALUES (null, '$name', 'uploaded',NULL, NOW(), NULL);";

if (!$conn->query($sql)) {
    print_r($conn->error);
}

$conn->close();