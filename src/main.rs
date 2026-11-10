#![windows_subsystem = "windows"]

use std::sync::mpsc::{channel, Receiver, Sender};

use anyhow::Result;
use eframe::egui;
use grammers_client::types::Chat;
use grammers_client::{Client, Config, InitParams, SignInError, Update};
use grammers_session::Session;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const SESSION_FILE: &str = "tg.session";

// ---------- Протокол между UI и бэкендом ----------

enum Cmd {
    Phone(String),
    Code(String),
    Password(String),
    Open(usize),
    Send(String),
}

#[derive(Clone)]
struct Msg {
    out: bool,
    from: String,
    text: String,
}

enum Ev {
    NeedPhone,
    NeedCode,
    NeedPassword,
    Ready,
    Dialogs(Vec<String>),
    History(Vec<Msg>),
    Message(Msg),
    Error(String),
}

// ---------- Бэкенд (tokio + grammers) ----------

async fn backend(
    mut cmd_rx: UnboundedReceiver<Cmd>,
    ev_tx: Sender<Ev>,
    ctx: egui::Context,
) -> Result<()> {
    let emit = |e: Ev| {
        let _ = ev_tx.send(e);
        ctx.request_repaint();
    };

    let api_id: i32 = std::env::var("TG_API_ID")?.parse()?;
    let api_hash = std::env::var("TG_API_HASH")?;

    let client = Client::connect(Config {
        session: Session::load_file_or_create(SESSION_FILE)?,
        api_id,
        api_hash,
        params: InitParams::default(),
    })
    .await?;

    if !client.is_authorized().await? {
        emit(Ev::NeedPhone);
        let Some(Cmd::Phone(phone)) = cmd_rx.recv().await else { return Ok(()) };
        let token = client.request_login_code(&phone).await?;

        emit(Ev::NeedCode);
        let Some(Cmd::Code(code)) = cmd_rx.recv().await else { return Ok(()) };
        match client.sign_in(&token, &code).await {
            Ok(_) => {}
            Err(SignInError::PasswordRequired(pt)) => {
                emit(Ev::NeedPassword);
                let Some(Cmd::Password(pw)) = cmd_rx.recv().await else { return Ok(()) };
                client.check_password(pt, pw.trim()).await?;
            }
            Err(e) => return Err(e.into()),
        }
        client.session().save_to_file(SESSION_FILE)?;
    }

    emit(Ev::Ready);

    // Список диалогов (первые 100)
    let mut chats: Vec<Chat> = Vec::new();
    let mut it = client.iter_dialogs();
    while let Some(d) = it.next().await? {
        chats.push(d.chat().clone());
        if chats.len() >= 100 {
            break;
        }
    }
    emit(Ev::Dialogs(chats.iter().map(|c| c.name().to_string()).collect()));

    let mut current: Option<usize> = None;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                match cmd {
                    Cmd::Open(i) if i < chats.len() => {
                        current = Some(i);
                        let mut msgs = Vec::new();
                        let mut it = client.iter_messages(&chats[i]).limit(50);
                        while let Some(m) = it.next().await? {
                            msgs.push(Msg {
                                out: m.outgoing(),
                                from: m.sender().map(|s| s.name().to_string()).unwrap_or_default(),
                                text: m.text().to_string(),
                            });
                        }
                        msgs.reverse(); // от старых к новым
                        emit(Ev::History(msgs));
                    }
                    Cmd::Send(text) => {
                        if let Some(i) = current {
                            match client.send_message(&chats[i], text.as_str()).await {
                                Ok(_) => emit(Ev::Message(Msg { out: true, from: "Вы".into(), text })),
                                Err(e) => emit(Ev::Error(e.to_string())),
                            }
                        }
                    }
                    _ => {}
                }
            }
            upd = client.next_update() => {
                if let Update::NewMessage(m) = upd? {
                    let is_current = current.map_or(false, |i| chats[i].id() == m.chat().id());
                    if is_current && !m.outgoing() {
                        emit(Ev::Message(Msg {
                            out: false,
                            from: m.sender().map(|s| s.name().to_string()).unwrap_or_default(),
                            text: m.text().to_string(),
                        }));
                    }
                }
            }
        }
    }
    Ok(())
}

// ---------- UI (egui) ----------

#[derive(PartialEq)]
enum Stage {
    Connecting,
    Phone,
    Code,
    Password,
    Chats,
}

struct App {
    cmd_tx: UnboundedSender<Cmd>,
    ev_rx: Receiver<Ev>,
    stage: Stage,
    input: String,
    dialogs: Vec<String>,
    selected: Option<usize>,
    messages: Vec<Msg>,
    error: Option<String>,
}

impl App {
    fn send(&self, c: Cmd) {
        let _ = self.cmd_tx.send(c);
    }

    fn poll(&mut self) {
        while let Ok(ev) = self.ev_rx.try_recv() {
            match ev {
                Ev::NeedPhone => self.stage = Stage::Phone,
                Ev::NeedCode => self.stage = Stage::Code,
                Ev::NeedPassword => self.stage = Stage::Password,
                Ev::Ready => self.stage = Stage::Chats,
                Ev::Dialogs(d) => self.dialogs = d,
                Ev::History(h) => self.messages = h,
                Ev::Message(m) => self.messages.push(m),
                Ev::Error(e) => self.error = Some(e),
            }
        }
    }

    fn login_form(&mut self, ui: &mut egui::Ui, label: &str, mk: fn(String) -> Cmd, secret: bool) {
        ui.add_space(40.0);
        ui.heading("Telegram");
        ui.label(label);
        let resp = ui.add(egui::TextEdit::singleline(&mut self.input).password(secret));
        let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
        if ui.button("Далее").clicked() || enter {
            let v = std::mem::take(&mut self.input);
            self.send(mk(v));
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        self.poll();

        if let Some(err) = self.error.clone() {
            egui::TopBottomPanel::top("err").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.colored_label(egui::Color32::RED, err);
                    if ui.button("✕").clicked() {
                        self.error = None;
                    }
                });
            });
        }

        match self.stage {
            Stage::Connecting => {
                egui::CentralPanel::default().show(ctx, |ui| ui.label("Подключение…"));
            }
            Stage::Phone => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    self.login_form(ui, "Номер телефона (+79991234567):", Cmd::Phone, false)
                });
            }
            Stage::Code => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    self.login_form(ui, "Код из Telegram:", Cmd::Code, false)
                });
            }
            Stage::Password => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    self.login_form(ui, "Пароль 2FA:", Cmd::Password, true)
                });
            }
            Stage::Chats => {
                egui::SidePanel::left("chats").default_width(260.0).show(ctx, |ui| {
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        for (i, name) in self.dialogs.clone().iter().enumerate() {
                            if ui.selectable_label(self.selected == Some(i), name).clicked() {
                                self.selected = Some(i);
                                self.messages.clear();
                                self.send(Cmd::Open(i));
                            }
                        }
                    });
                });

                egui::TopBottomPanel::bottom("input").show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        let resp = ui.add(
                            egui::TextEdit::singleline(&mut self.input)
                                .desired_width(ui.available_width() - 80.0)
                                .hint_text("Сообщение…"),
                        );
                        let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                        if (ui.button("Отправить").clicked() || enter)
                            && !self.input.trim().is_empty()
                            && self.selected.is_some()
                        {
                            let text = std::mem::take(&mut self.input);
                            self.send(Cmd::Send(text));
                            resp.request_focus();
                        }
                    });
                });

                egui::CentralPanel::default().show(ctx, |ui| {
                    egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
                        for m in &self.messages {
                            let layout = if m.out {
                                egui::Layout::right_to_left(egui::Align::TOP)
                            } else {
                                egui::Layout::left_to_right(egui::Align::TOP)
                            };
                            ui.with_layout(layout, |ui| {
                                ui.group(|ui| {
                                    ui.set_max_width(ui.available_width() * 0.7);
                                    if !m.out {
                                        ui.strong(&m.from);
                                    }
                                    ui.label(&m.text);
                                });
                            });
                        }
                    });
                });
            }
        }
    }
}

fn main() -> eframe::Result<()> {
    eframe::run_native(
        "Telegram (Rust)",
        eframe::NativeOptions::default(),
        Box::new(|cc| {
            let (cmd_tx, cmd_rx) = unbounded_channel();
            let (ev_tx, ev_rx) = channel();
            let ctx = cc.egui_ctx.clone();

            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let err_tx = ev_tx.clone();
                    let err_ctx = ctx.clone();
                    if let Err(e) = backend(cmd_rx, ev_tx, ctx).await {
                        let _ = err_tx.send(Ev::Error(format!("{e:#}")));
                        err_ctx.request_repaint();
                    }
                });
            });

            Ok(Box::new(App {
                cmd_tx,
                ev_rx,
                stage: Stage::Connecting,
                input: String::new(),
                dialogs: vec![],
                selected: None,
                messages: vec![],
                error: None,
            }))
        }),
    )
}
