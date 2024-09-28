const dropZone = document.getElementById('input-file-container');
const statusText = document.getElementById('status-text');
const imgInput = document.getElementById('img-input');
const videoInput = document.getElementById('video-input');
const relatedPics = document.querySelector('.relatedPics');
const instruction = document.querySelector('.instruction');
const progressbar = document.querySelector('.progress-bar');
const fileinput = document.getElementById('file');
let file;

if(document.getElementById) {
    window.alert = function(txt, code) {
        createCustomAlert(txt, code);
    }
}

function displayFile() {
    let fileType = file.type;
    // console.log(fileType);

    let validExtensions = ["video/mp4", "video/webm", "video/amv", "video/flv"];

    if (validExtensions.includes(fileType)) {
        let fileReader = new FileReader();

        fileReader.onload = (event) => {
            let fileURL = event.target.result;
            let imgTag = `<video style="width: 100%; height: 100%;" controls><source src="${fileURL}" type="video/mp4"></video>`;
            imgInput.innerHTML = imgTag;
            imgInput.removeAttribute('hidden');
        };
        fileReader.readAsDataURL(file);
        alert("Загружено", 200);

        return true;
    } else {
        alert("Это не видео!!!", 418);

        return false;
    }
}

fileinput.addEventListener('input', function (e) {
    file = (fileinput.files)[0];

    if(displayFile()) {
        file.disabled = true;
        const formdata = new FormData();
        const xhr = new XMLHttpRequest()
        formdata.append('video', file)

        xhr.open('post', 'upload_video.php', false);
        xhr.send(formdata);

        if (xhr.status != 200) {
            // обработать ошибку
            alert( xhr.status + ': ' + xhr.statusText, xhr.status); // пример вывода: 404: Not Found
        } else {
            statusText.textContent = 'Загружено 100%';
            progressbar.value = 100;
            const formdata2 = new FormData();
            formdata2.append('video', file.name);
            let xhr2 = new XMLHttpRequest();

            let interval = setInterval(() => {
                xhr2.open('post', 'get_proccessed.php', false);
                xhr2.send(formdata2);
                statusText.textContent = 'Обработка';
                if (xhr2.status == 200) {
                    console.log(xhr2.response)
                    let JSONobj = JSON.parse(xhr2.response)
                    console.log(JSONobj.link)
                    console.log((JSONobj.id)[0])
                    statusText.textContent = 'Обработано 100%';
                    let formData3 = new FormData();
                    formData3.append('link', JSONobj.link);
                    formData3.append('id', (JSONobj.id)[0])
                    let xhr3 = new XMLHttpRequest();
                    xhr3.open('post', 'check_duplicated.php', false);
                    xhr3.send(formData3);
                    console.log(xhr3)
                    clearInterval(interval);
                    if (xhr3.status != 200) {
                        statusText.textContent = 'Ошибка обработки';
                        document.getElementById('relVid').innerHTML = 'Ошибка обработки'
                    } else {
                        document.getElementById('foobar').play();
                        document.getElementById('relVid').innerHTML = xhr3.response ? `<video style="width: 100%; height: 100%;" controls><source src="${xhr3.response}" type="video/mp4"></video>` : 'Нет похожих видео'
                    }
                }
            }, 10000)

            if(interval) {
                // вывести результат
                alert(xhr.responseText, xhr.status);
            }
        }

        setTimeout(() => {

            instruction.setAttribute('hidden', true);
            relatedPics.removeAttribute('hidden');
            relatedPics.style.display = 'flex';
        }, 2000)
    }
})

if (dropZone) {

    dropZone.addEventListener("dragenter", function(e) {
        e.preventDefault();
    });

    dropZone.addEventListener("dragover", function(e) {
        e.preventDefault();
    });

    dropZone.addEventListener("dragleave", function(e) {
        e.preventDefault();
    });

    dropZone.addEventListener("drop", function(e) {
        e.preventDefault();
        file = e.dataTransfer.files[0];

        if(displayFile()) {
            file.disabled = true;
            instruction.setAttribute('hidden', true);
            const xhr = new XMLHttpRequest();
            xhr.open('post', 'upload_video.php', false);

            for (let i = 0; i <= 100; i++) {
                updateProgress(i, 100, 'Загружено');
            }

            setTimeout(() => {
                for (let i = 0; i <= 100; i++) {
                    updateProgress(i, 100, 'Обработано');
                }

                document.getElementById('foobar').play();
                relatedPics.removeAttribute('hidden');
                relatedPics.style.display = 'flex';
            }, 2000)
        }
    });
}

function updateProgress(loaded, total, txt) {
    const percentLoaded = Math.round((loaded / total) * 100)

    progressbar.value = percentLoaded
    statusText.textContent = `${txt} ${percentLoaded}%`
}

var ALERT_TITLE = "Упс!";
var ALERT_BUTTON_TEXT = "Ок";

function createCustomAlert(txt, code) {
    d = document;

    const fade_in = [
        { opacity: 0 },
        { opacity: 1 },
    ];

    const fade_out = [
        { opacity: 1 },
        { opacity: 0 },
    ];

    const fadeOptions = {
        duration: 1000,
    };

    let codes = new Map([
        [200, 'green'],
        [404, 'red'],
        [403, 'red'],
        [418, 'red'],
    ]);

    if (code === 200) {
        ALERT_TITLE = "Отлично!";
    } else {
        ALERT_TITLE = "Упс!";
    }

    if(d.getElementById("modalContainer")) return;

    mObj = d.getElementsByTagName("body")[0].appendChild(d.createElement("div"));
    mObj.id = "modalContainer";
    mObj.style.height = d.documentElement.scrollHeight + "px";

    alertObj = mObj.appendChild(d.createElement("div"));
    alertObj.id = "alertBox";
    if(d.all && !window.opera) alertObj.style.top = document.documentElement.scrollTop + "px";
    alertObj.style.left = (d.documentElement.scrollWidth - alertObj.offsetWidth)/2 + "px";
    alertObj.style.visiblity="visible";

    h1 = alertObj.appendChild(d.createElement("h1"));
    h1.appendChild(d.createTextNode(ALERT_TITLE));
    hr = alertObj.appendChild(d.createElement("hr"))
    hr.style.background = codes.get(code)
    msg = alertObj.appendChild(d.createElement("p"));
    //msg.appendChild(d.createTextNode(txt));
    msg.innerHTML = txt;

    btn = alertObj.appendChild(d.createElement("a"));
    btn.id = "closeBtn";
    btn.appendChild(d.createTextNode(ALERT_BUTTON_TEXT));
    btn.href = "#";
    btn.focus();
    alertObj.animate(fade_in, fadeOptions);
    btn.onclick = function() {
        alertObj.animate(fade_out, fadeOptions);
        setTimeout(() => {
            removeCustomAlert();
            return false;
            }, 900)
    }

    alertObj.style.display = "block";

}

function removeCustomAlert() {
    document.getElementsByTagName("body")[0].removeChild(document.getElementById("modalContainer"));
}

function beep() {
    var snd = new Audio("beautiful-sms-notification-sound.mp3");
    snd.play();
}