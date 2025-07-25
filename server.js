const express = require('express');
const app = express();
const PORT = 3000;


app.get('/health', (req, res)=>{

    res.send(200).json({'health':"ok"})
} 
)

app.listen(3000, ()=> console.log("Server running"))