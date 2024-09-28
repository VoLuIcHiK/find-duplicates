<?php
require_once('include/header.php');
define ('SITE_ROOT', realpath(dirname(__FILE__)));

$conn = mysqli_connect('mysql', 'root', 'root', 'yappy_db');
$videoData = mysqli_fetch_assoc($conn->query("SELECT * FROM video WHERE id = {$_GET['id']}"));
$conn->close();

?>
<main>
    <div class="container" style="display: flex; flex-direction: column; align-items: center; justify-content: center;">
        <div class="video"  style="
     display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    width: 100%;
    color: #00E2B8;
}">
            <h2 class="head" style="font-size: 36px; margin-bottom: 5%;">
                Rick Astley Never Gonna Give You Up
            </h2>
            <div class="pics" style="display: flex; flex-wrap: wrap; gap: 35px">
                <div class="card">
                    <div class="card-head">
                    </div>
                    <video width="900" height="500" controls>
                        <source src="http://localhost:8080/public/videos/<?= $videoData['file'] ?>" type="video/mp4">
                    </video>
                </div>
            </div>
        </div>
        <hr style="margin: 50px 0; background: #ddd; height: 2px; width: 100%; position: relative; z-index: 1000">
        <div class="desc" style="width: 100%">
            <h1 style="font-size: 24px; color: #00E2B8">Описание видео</h1>
            <details>
                <summary>Развернуть</summary>
                You got rickrolled :D !
            </details>
        </div>
        <div class="relatedPics"  style="
    flex-direction: column;
    margin-top: 10%;
    width: 100%;
    color: #00E2B8;
    align-items: center;
    display: flex;
    justify-content: center;">
            <h2 class="head" style="font-size: 36px; margin-bottom: 5%;">
                Похожий ролик
            </h2>
            <div class="vids relPics" style="display: flex; flex-wrap: wrap; gap: 15px">
                <?php if (!is_null($videoData['related_video'])): ?>
                <video width="900" height="500" controls>
                    <source src="<?= $videoData['related_video'] ?>" type="video/mp4">
                </video>
                <?php else: ?>
                <span>Нет похожих видео...</span>
                <?php endif; ?>
            </div>
        </div>
    </div>
</main>
<script src="script.js"></script>
</body>
</html>