<?php require_once('include/header.php');
$fastapi_url = 'http://projectvoid.my.to:8054/check-video-duplicate';
if (getenv('FASTAPI_URL')) $fastapi_url = $_ENV['FASTAPI_URL'];
$_ENV['HOSTNAME'] = 'localhost';
$conn = mysqli_connect('mysql', 'root', 'root', 'yappy_db');
$videos = $conn->query('SELECT * FROM video');
//var_dump($_SERVER['HTTP_HOST']);
?>

<main>
    <div class="container">
        <div class="allVids">
            <h2 class="head" style="font-size: 36px; margin-bottom: 5%;">
                Все видео
            </h2>
            <div class="vids relPics">
                <?php foreach ($videos as $video): ?>
                <a href="video-page.php/?id=<?= $video['id'] ?>" target="_blank">
                    <div class="card">
                        <div class="card-head">
                            Видео №<?= $video['id'] ?>
                        </div>
                        <video class="relVids">
                            <source src="public/videos/<?= $video['file'] ?>" type="video/mp4">
                        </video>
                    </div>
                </a>
                <?php endforeach; ?>
            </div>
        </div>
    </div>
</main>

<script src="script.js"></script>
</body>
</html>