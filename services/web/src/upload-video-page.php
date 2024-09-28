<?php require_once('include/header.php') ?>
<main>
    <div>
        <div class="status" style="margin-bottom: 5%;">
            <div class="status-text" id="status-text" style="color: #00E2B8"></div>
            <progress class="progress-bar" style="width: 100%; height: 0.25em;" value="0" max="100"></progress>
        </div>
        <audio id="foobar" src="public/sounds/beautiful-sms-notification-sound.mp3" preload="auto"></audio>
        <div class="file-input" id="input-file-container" style="position: relative;">
            <div style="width: 100%; height: 100%" id="img-input" hidden></div>
            <input type="file" id="file" hidden>
            <label for="file" id="input-file">Выберите видео или перетащите его сюда</label>
        </div>
    </div>
    <div class="instruction">
        <span style="color: #00E2B8">Инструкция:</span>
        <ol style="color: #00E2B8" >
            <li>Загрузите видео;</li>
            <li>Дождитесь окончания обработки;</li>
            <li>Посмотрите наиболее похожие видео</li>
        </ol>
    </div>
    <div class="relatedPics"  style="flex-direction: column; width: 35%; color: #00E2B8" hidden>
        <h2 class="head" style="font-size: 36px; margin-bottom: 5%;">
            Похожие ролики
        </h2>
        <div class="vids relPics" id="relVid" style="display: flex; flex-wrap: wrap; gap: 15px">

        </div>
    </div>
</main>
<script src="script.js"></script>
</body>
</html>