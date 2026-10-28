const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');
const app = express();
const server = http.createServer(app);
const io = new Server(server);
const PORT = process.env.PORT || 3000;
app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (_req, res) => res.json({ ok: true }));

const rooms = new Map();
const ROWS = 6, COLS = 7, GRACE_MS = 60000;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const board = () => Array.from({ length: ROWS }, () => Array(COLS).fill(null));
const cleanName = v => String(v || '').trim().replace(/\s+/g, ' ').slice(0, 18);
function makeCode() {
  let code;
  do { code = Array.from({ length: 5 }, () => CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]).join(''); }
  while (rooms.has(code));
  return code;
}
function publicRoom(r) {
  return { code:r.code, status:r.status, players:r.players.map(p => p ? {name:p.name,color:p.color,connected:!!p.socketId}:null), board:r.board, currentTurn:r.currentTurn, winner:r.winner, winningCells:r.winningCells, draw:r.draw, message:r.message, round:r.round };
}
function broadcast(r) { io.to(r.code).emit('room-state', publicRoom(r)); }
function roomFor(s) { return s.data.roomCode ? rooms.get(s.data.roomCode) : null; }
function wins(b, row, col, color) {
  for (const [dr,dc] of [[0,1],[1,0],[1,1],[1,-1]]) {
    const cells=[[row,col]];
    for (const sign of [-1,1]) {
      let r=row+dr*sign,c=col+dc*sign;
      while(r>=0&&r<ROWS&&c>=0&&c<COLS&&b[r][c]===color){cells.push([r,c]);r+=dr*sign;c+=dc*sign;}
    }
    if(cells.length>=4) return cells;
  }
  return null;
}
function disconnectPlayer(s, reason='disconnected') {
  const r=roomFor(s); if(!r) return;
  const p=r.players.find(x=>x&&x.socketId===s.id); s.data.roomCode=null;
  if(!p) return;
  p.socketId=null; p.disconnectedAt=Date.now();
  r.message=`${p.name} ${reason}. They have 60 seconds to reconnect.`; broadcast(r);
  clearTimeout(p.timer);
  p.timer=setTimeout(()=>{
    const current=rooms.get(r.code)?.players.find(x=>x&&x.token===p.token);
    if(!current||current.socketId||!current.disconnectedAt) return;
    if(r.status==='playing') { r.status='waiting'; r.message=`${p.name} did not reconnect in time. Waiting for them to return.`; }
    broadcast(r);
  }, GRACE_MS);
}
function ack(cb, data) { if(typeof cb==='function') cb(data); }
io.on('connection', socket => {
  socket.on('create-room', ({name}={}, cb) => {
    name=cleanName(name); if(!name) return ack(cb,{ok:false,error:'Enter a nickname first.'});
    const code=makeCode(), token=crypto.randomBytes(24).toString('hex');
    const r={code,status:'waiting',players:[{name,color:'red',socketId:socket.id,token,disconnectedAt:null,timer:null},null],board:board(),currentTurn:0,winner:null,winningCells:[],draw:false,message:'Waiting for a second player to join.',round:1,rematchVotes:new Set()};
    rooms.set(code,r); socket.join(code); socket.data.roomCode=code; socket.data.playerToken=token;
    ack(cb,{ok:true,code,token,seat:0,state:publicRoom(r)}); broadcast(r);
  });
  socket.on('join-room', ({code,name}={}, cb) => {
    name=cleanName(name); code=String(code||'').trim().toUpperCase();
    if(!name) return ack(cb,{ok:false,error:'Enter a nickname first.'});
    if(!/^[A-HJ-NP-Z2-9]{5}$/.test(code)) return ack(cb,{ok:false,error:'Invalid room code. Use the 5-character code.'});
    const r=rooms.get(code); if(!r) return ack(cb,{ok:false,error:'Room not found. Ask the host for a fresh code or link.'});
    const token=socket.handshake.auth?.playerToken || socket.data.playerToken;
    let seat=r.players.findIndex(p=>p&&p.token===token);
    if(seat>=0) {
      const p=r.players[seat];
      if(p.socketId&&p.socketId!==socket.id) { const old=io.sockets.sockets.get(p.socketId); if(old) old.disconnect(true); }
      clearTimeout(p.timer); p.socketId=socket.id; p.disconnectedAt=null;
    } else {
      seat=r.players.findIndex(p=>p===null);
      if(seat<0) return ack(cb,{ok:false,error:'This room already has two players. Create a new room.'});
      r.players[seat]={name,color:seat===0?'red':'yellow',socketId:socket.id,token:crypto.randomBytes(24).toString('hex'),disconnectedAt:null,timer:null};
    }
    socket.join(code); socket.data.roomCode=code; socket.data.playerToken=r.players[seat].token;
    if(r.players.every(p=>p&&p.socketId)&&r.status==='waiting') { r.status='playing'; r.message='Game on! Red goes first.'; }
    else r.message=`${r.players[seat].name} joined the room.`;
    ack(cb,{ok:true,code,token:r.players[seat].token,seat,state:publicRoom(r)}); broadcast(r);
  });
  socket.on('make-move', ({column}={}, cb) => {
    const r=roomFor(socket); if(!r) return ack(cb,{ok:false,error:'You are not in a room.'});
    if(r.status!=='playing') return ack(cb,{ok:false,error:'The game is not accepting moves right now.'});
    const seat=r.players.findIndex(p=>p&&p.socketId===socket.id);
    if(seat<0) return ack(cb,{ok:false,error:'Reconnect to your player seat.'});
    if(seat!==r.currentTurn) return ack(cb,{ok:false,error:'It is not your turn yet.'});
    if(!Number.isInteger(column)||column<0||column>=COLS) return ack(cb,{ok:false,error:'Invalid column.'});
    let row=ROWS-1; while(row>=0&&r.board[row][column]!==null) row--;
    if(row<0) return ack(cb,{ok:false,error:'That column is full. Choose another.'});
    const color=r.players[seat].color; r.board[row][column]=color;
    const line=wins(r.board,row,column,color);
    if(line){r.status='finished';r.winner=seat;r.winningCells=line;r.message=`${r.players[seat].name} wins!`;}
    else if(r.board.every(line=>line.every(cell=>cell!==null))){r.status='finished';r.draw=true;r.message="It's a draw! The board is full.";}
    else {r.currentTurn=1-seat;r.message=`${r.players[r.currentTurn].name}'s turn.`;}
    ack(cb,{ok:true}); broadcast(r);
  });
  socket.on('play-again', (_data, cb) => {
    const r=roomFor(socket); if(!r) return ack(cb,{ok:false,error:'You are not in a room.'});
    const seat=r.players.findIndex(p=>p&&p.socketId===socket.id);
    if(seat<0) return ack(cb,{ok:false,error:'Reconnect before starting another round.'});
    if(r.status!=='finished') return ack(cb,{ok:false,error:'Finish this round first.'});
    r.rematchVotes.add(seat); r.message=r.rematchVotes.size===2?'Starting a new round!':`${r.players[seat].name} wants to play again. Waiting for the other player.`;
    if(r.rematchVotes.size===2&&r.players.every(p=>p&&p.socketId)) {r.board=board();r.currentTurn=0;r.winner=null;r.winningCells=[];r.draw=false;r.status='playing';r.round++;r.rematchVotes.clear();r.message='New round! Red goes first.';}
    ack(cb,{ok:true}); broadcast(r);
  });
  socket.on('leave-room', () => disconnectPlayer(socket,'left the room'));
  socket.on('disconnect', () => disconnectPlayer(socket));
});
server.listen(PORT,'0.0.0.0',()=>console.log(`Connect Four Online running on port ${PORT}`));
