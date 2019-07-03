(ns juxt.crux-ui.server.preloader)

(def ^:private style
[:style
 "html, body, #app, .preloader {
width: 100%;
height: 100%;
}

.preloader {
display: flex;
align-items: center;
justify-content: center;
}

.scene {
width: 200px;
height: 200px;
perspective: 600px;
}

.cube {
width: 100%;
height: 100%;
position: relative;
transform-style: preserve-3d;
transform: translateZ(-100px) rotateX(-45deg) rotateY(-45deg);
}

.cube__face {
position: absolute;
width: 200px;
height: 200px;
border: 1px solid orange;
}

.cube__face--front  { transform: rotateY(  0deg); }
.cube__face--right  { transform: rotateY( 90deg); }
.cube__face--back   { transform: rotateY(180deg); }
.cube__face--left   { transform: rotateY(-90deg); }
.cube__face--top    { transform: rotateX( 90deg); }
.cube__face--bottom { transform: rotateX(-90deg); }


.cube__face--front  { transform: rotateY(  0deg) translateZ(100px); }
.cube__face--right  { transform: rotateY( 90deg) translateZ(100px); }
.cube__face--back   { transform: rotateY(180deg) translateZ(100px); }
.cube__face--left   { transform: rotateY(-90deg) translateZ(100px); }
.cube__face--top    { transform: rotateX( 90deg) translateZ(100px); }
.cube__face--bottom { transform: rotateX(-90deg) translateZ(100px); }

.cube__face.cube__face--back {}

.cube__face {
border-color: gray !important;
}


@keyframes pulse-bottom-left {
0% {
border-bottom: 1px solid gray;
border-left: 1px solid gray;
}
25% {
border-bottom: 1px solid orange;
border-left: 1px solid orange;
}
50% {
border-left: 1px solid gray;
border-top: 1px solid gray;
}
}
@keyframes pulse-bottom-right {
0% {
border-bottom: 1px solid gray;
border-right: 1px solid gray;
}
25% {
border-bottom: 1px solid orange;
border-right: 1px solid orange;
}
50% {
border-left: 1px solid gray;
border-top: 1px solid gray;
}
}
@keyframes pulse-top-right {
0% {
border-top: 1px solid gray;
border-right: 1px solid gray;
}
25% {
border-top: 1px solid orange;
border-right: 1px solid orange;
}
50% {
border-left: 1px solid gray;
border-top: 1px solid gray;
}
}
@keyframes pulse-top-left {
0% {
border-left: 1px solid gray;
border-top: 1px solid gray;
}
25% {
border-left: 1px solid orange;
border-top: 1px solid orange;
}
50% {
border-left: 1px solid gray;
border-top: 1px solid gray;
}
}


.cube__face--front {
animation: pulse-top-right 8s 0s infinite, pulse-bottom-right 8s 2s infinite;
}
.cube__face--right {
animation: pulse-top-left 8s 0s infinite, pulse-bottom-left 8s 2s infinite;
}
.cube__face--top {
animation: pulse-bottom-right 8s 0s infinite, pulse-top-left 8s 6s infinite;
}
.cube__face--bottom {
animation: pulse-top-right 8s 2s infinite, pulse-bottom-left 8s 4s infinite;
}
.cube__face--back {
animation: pulse-bottom-right 8s 4s infinite, pulse-top-right 8s 6s infinite;
}
.cube__face--left {
animation: pulse-bottom-left 8s 4s infinite, pulse-top-left 8s 6s infinite;
}"])

(def root
  [:div.preloader
   style
   [:div.scene
    (let [titles? false]
        [:div.cube
         [:div.cube__face.cube__face--front  (if titles? "front")]
         [:div.cube__face.cube__face--back   (if titles? "back")]
         [:div.cube__face.cube__face--right  (if titles? "right")]
         [:div.cube__face.cube__face--left   (if titles? "left")]
         [:div.cube__face.cube__face--top    (if titles? "top")]
         [:div.cube__face.cube__face--bottom (if titles? "bottom")]])]])
