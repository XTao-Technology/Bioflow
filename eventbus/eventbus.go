package eventbus

import (
    "sync"
    "errors"
    "net/smtp"
    "strings"
    "bytes"
    "net/http"
    "crypto/tls"
)

var INIT int = 0
var RUN  int = 1
var STOP int = 2

var THRESHOLD int = 5000
var WORKERNUM int = 10

var globalBus *EventBus
var globalMailSender *mailSender
var globalLock *sync.Mutex = new(sync.Mutex)

type Event interface{
    SetId(string)
    GetId() string
    SetData(interface{})error
    GetData() interface{}
}

type BaseEvent struct {
    id string
    data interface{}
}

func (e *BaseEvent) SetId(id string) {
    e.id = id
}

func (e *BaseEvent) GetId() string {
    return e.id
}

func (e *BaseEvent) SetData(a interface{}) error {
    e.data = a
    return nil
}

func (e *BaseEvent) GetData() interface{} {
    return e.data
}

func NewBaseEvent(id string, data interface{}) *BaseEvent {
    return &BaseEvent{
        id: id,
        data: data,
    }
}

type mailSender struct {
    user string
    password string
    host string
}

//User can set a global mail sender, subscribers can use it to send e-mails.
func SetMailSender(user, password, host string) {
    if user == "" || password == "" || host == "" {
        return
    }
    sender := &mailSender{
        user:user,
        password:password,
        host:host,
    }
    globalLock.Lock()
    globalMailSender = sender
    globalLock.Unlock()
}

type MailReceiver struct{
    address string
    msg string
    subject string
    fnc func(interface{})string
}

//The parameter 'a' indicate that how we decide the content of mail we will send.
//It can be a string or a function that take the result of event.GetData() and
//return a string.
func NewMailReceiver(address, subject string, a interface{}) (*MailReceiver, error) {
    if address =="" || a == nil{
        return nil , errors.New("Receiver's address and last parameter can't be empty.")
    }
    receiver := &MailReceiver{
        address: address,
        msg: "",
        subject: subject,
        fnc: nil,
    }

    if msg, ok := a.(string); ok {
        receiver.msg = msg
        return receiver, nil
    }

    if fnc, ok := a.(func(interface{})string); ok {
        receiver.fnc = fnc
        return receiver, nil
    }

    return nil, errors.New("The last parameter must be string or func(interface{})string")
}

func (receiver *MailReceiver) TryReceive (data interface{}) error {
    if globalMailSender == nil {
        return errors.New("There is no e-mail sender.")
    }
    mailNotice := new(MailNotice)
    mailNotice.receiver = receiver
    mailNotice.sender = globalMailSender
    return mailNotice.TrySend(data)

}

type MailNotice struct{
    sender *mailSender
    receiver *MailReceiver
}

func NewMailNotice (user, password, host, to, subject string, a interface{}) (*MailNotice, error) {

    if user == "" || password == "" || host == "" || to == "" || a == nil{
        return nil, errors.New("There can't be empty parameter.")
    }

    mailNotice := new(MailNotice)
    receiver, _ := NewMailReceiver(to, subject, a)
    if receiver == nil {
        return nil, errors.New("The last parameter must be string or func(interface{})string.")
    }

    mailNotice.receiver = receiver
    sender := new(mailSender)
    sender.user = user
    sender.password = password
    sender.host = host
    mailNotice.sender = sender
    return mailNotice, nil
}

func (mail *MailNotice) TrySend (data interface{}) error{
    if mail.sender == nil || mail.receiver == nil {
        return errors.New("There is no e-mail sender or receiver.")
    }
    user := mail.sender.user
    password := mail.sender.password
    host := mail.sender.host
    to := mail.receiver.address
    subject := mail.receiver.subject
    var msg string
    if mail.receiver.fnc != nil {
        msg = mail.receiver.fnc(data)
    }else {
        msg = mail.receiver.msg
    }
    contentType := "Content-Type: text/plain; charset=UTF-8"
    aHost := (strings.Split(host, ":"))[0]
    auth := smtp.PlainAuth("", user, password, aHost)
    content := []byte("To: " + to + "\r\nFrom: " + user + ">\r\nSubject: " + subject + "\r\n" + contentType + "\r\n\r\n" + msg)
    sendTo := strings.Split(to, ";")
    err := smtp.SendMail(host, auth, user, sendTo, content)
    return err
}

func (mail *MailNotice) send(data interface{}) {
    mail.TrySend(data)
}

type RestAPI struct {
    url string
    body []byte
    fnc func(interface{}) []byte
}

//The parameter 'a' indicate that how we decide the content of API we will send.
//It can be a string or a function that take the result of event.GetData() and
//return a []byte.
//NOTICE:The content must be json format.
func NewRestAPI(url string, a interface{}) (*RestAPI, error) {
    if url == "" || a == nil{
        return nil, errors.New("Parameters can't be empty.")
    }

    restAPI := &RestAPI{
        url: url,
        fnc: nil,
    }

    if body, ok := a.([]byte); ok {
        restAPI.body = body
        return restAPI, nil
    }else if fnc, ok := a.(func(interface{})[]byte); ok {
        restAPI.fnc = fnc
        return restAPI, nil
    }else{
        return nil, errors.New("The second parameter must be string or func(interface{})string")
    }
}

func (restAPI *RestAPI) TryCall(a interface{}) (*http.Response, error) {
    var body []byte
    if restAPI.fnc != nil {
        body = restAPI.fnc(a)
    }else{
        body = restAPI.body
    }

    content := bytes.NewBuffer(body)

    req, err := http.NewRequest("POST", restAPI.url, content)
    req.Header.Set("Content-type", "application/json")
    if err != nil {
        return nil, err
    }

    url := strings.ToUpper(restAPI.url)
    if strings.HasPrefix(url, "HTTPS") {
        trpt := &http.Transport{
            TLSClientConfig: &tls.Config{
                InsecureSkipVerify: true,
            },
        }
        clt := &http.Client{Transport: trpt}
        resp, err := clt.Do(req)
        return resp, err
    } else {
        client := &http.Client{}
        re, err := client.Do(req)
        return re, err
    }
}

func (restAPI *RestAPI) call(a interface{}) {
    restAPI.TryCall(a)
}

type callback struct {
    syn bool
    handler func(interface{})
}

func newCallback(syn bool, handler func(interface{})) *callback {
    c := &callback{
        syn: syn,
        handler: handler,
    }

    return c
}

type Subscriber struct{
    subscription map[string]*callback
    synCh chan Event
    asynCh chan Event
    status int
    lock *sync.RWMutex
}

func (s *Subscriber) handle(e Event) {
    s.lock.RLock()
    callback, ok := s.subscription[e.GetId()]
    if !ok {
        s.lock.RUnlock()
        return
    }
    s.lock.RUnlock()
    callback.handler(e.GetData())

    return
}

func (s *Subscriber) get(e Event){
    s.lock.RLock()
    call, ok := s.subscription[e.GetId()]
    if !ok {
        s.lock.RUnlock()
        return
    }
    s.lock.RUnlock()
    if call.syn {
        s.synCh <- e
    }else{
        s.asynCh <- e
    }
    return
}

func (s *Subscriber) watch(ch chan Event) {
    for {
        e :=<- ch
        if e == nil{
            return
        }
        s.handle(e)
    }
}

func (s *Subscriber) run() {
    go s.watch(s.synCh)
    for i := 0; i < WORKERNUM; i++ {
        go s.watch(s.asynCh)
    }
}

func NewSubscriber() *Subscriber{
    s := &Subscriber{
        subscription: make(map[string]*callback),
        synCh: make(chan Event, THRESHOLD),
        asynCh: make(chan Event, THRESHOLD),
        status: INIT,
        lock: new(sync.RWMutex),
    }
    return s
}

//The parameter 'a' is the type of handler,
// it's type can be one of func(interface{}), *MailReceiver, *MailNotice, *RestAPI.
func (s *Subscriber) Subscribe(eventId string, a interface{}, syn bool) error {
    if eventId == "" {
        return errors.New("Can't subscribe event with empty id.")
    }

    var callback *callback
    if f, ok := a.(func(interface{})); ok {
        callback = newCallback(syn, f)
    }else if api, ok := a.(*RestAPI); ok {
        callback = newCallback(syn, api.call)
    }else{
        var mail *MailNotice
        if receiver, ok := a.(*MailReceiver); ok {
            if globalMailSender == nil {
                return errors.New("There is no mail sender setting.")
            }

            mail = new(MailNotice)
            mail.receiver = receiver
            mail.sender = globalMailSender
        }else if m, ok := a.(*MailNotice); ok {
            mail = m
        }
        callback = newCallback(syn, mail.send)
    }

    if callback == nil{
        return errors.New("Type of the third parameter must be one of func(interface{}), *MailReceiver, *MailNotice, *RestAPI.")
    }

    s.lock.Lock()
    if _, ok := s.subscription[eventId]; ok {
        s.lock.Unlock()
        return errors.New("The event is already subscribed.")
    }
    s.subscription[eventId] = callback
    getBus().register(s, eventId)
    if len(s.subscription) == 1{
        s.run()
    }
    s.lock.Unlock()

    return nil

}

func (s *Subscriber) Unsubscribe(eventId string) error{
    if eventId == "" {
        return errors.New("Can't unsubscribe event with empty id.")
    }

    s.lock.Lock()
    if _, ok := s.subscription[eventId]; !ok {
        s.lock.Unlock()
        return nil
    }
    getBus().unregister(s, eventId)
    delete(s.subscription, eventId)
    if len(s.subscription) == 0{
        //Notify the watchers to stop.
        s.synCh <- nil
        for i := 0;i < WORKERNUM; i++ {
            s.asynCh <- nil
        }
    }
    s.lock.Unlock()
    return nil
}

func (s *Subscriber) UnsubscribeAll() {
    idList := s.SubscribedEvents()
    for _, eventId := range(idList) {
        s.Unsubscribe(eventId)
    }
}

func (s *Subscriber) SubscribedEvents() []string {
    list := make([]string, 0)
    s.lock.RLock()
    for eventId, _ := range(s.subscription) {
        list = append(list, eventId)
    }
    s.lock.RUnlock()
    return list
}

type EventBus struct{
    subscribers map[string]map[*Subscriber]bool
    lock *sync.RWMutex
}

func newEventBus() *EventBus {
    bus := &EventBus{
        subscribers: make(map[string]map[*Subscriber]bool),
        lock: new(sync.RWMutex),
    }

    return bus
}

func getBus() *EventBus {
    globalLock.Lock()
    if globalBus == nil {
        globalBus = newEventBus()
    }
    globalLock.Unlock()
    return globalBus
}

func (bus *EventBus) register(s *Subscriber, eventId string) {
    bus.lock.Lock()
    if m, ok := bus.subscribers[eventId]; ok{
        m[s] = true
    }else{
        newMap := make(map[*Subscriber]bool)
        newMap[s] = true
        bus.subscribers[eventId] = newMap
    }
    bus.lock.Unlock()
}

func (bus *EventBus) unregister(s *Subscriber, eventId string) {
    bus.lock.Lock()
    delete(bus.subscribers[eventId], s)
    if len(bus.subscribers[eventId]) == 0 {
        delete(bus.subscribers, eventId)
    }
    bus.lock.Unlock()
}

func (bus *EventBus) publish(e Event) error{
    if e == nil {
        return errors.New("Can't publish empty event.")
    }

    id := e.GetId()
    if id == "" {
        return errors.New("Can't publish event with empty id.")
    }

    bus.lock.RLock()
    subscribers := bus.subscribers[e.GetId()]
    bus.lock.RUnlock()
    for subscriber, _ := range subscribers {
        subscriber.get(e)
    }
    return nil
}

func Publish (e Event) error {
    return getBus().publish(e)
}
