#!/usr/bin/env python3
"""Generate the 12-slide Kafka PoC presentation (standard Office Open XML)."""
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE

p = Presentation()
p.slide_width, p.slide_height = Inches(13.333), Inches(7.5)
blank = p.slide_layouts[6]
NAVY, TEAL, BLUE, GRAY, PALE, WHITE = "14263D", "008F87", "2859C2", "526379", "F2F6FA", "FFFFFF"

def box(sl, x, y, w, h, color=PALE):
    sh=sl.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    sh.fill.solid(); sh.fill.fore_color.rgb=RGBColor.from_string(color)
    sh.line.fill.background()
    return sh

def text(sl, value, x, y, w, h, size=19, color=NAVY, bold=False, font="Calibri"):
    sh=sl.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame=sh.text_frame; frame.clear(); frame.word_wrap=True
    frame.margin_left=frame.margin_right=Inches(.04)
    frame.margin_top=frame.margin_bottom=Inches(.02)
    for i,line in enumerate(value.split("\n")):
        pr=frame.paragraphs[0] if i==0 else frame.add_paragraph()
        pr.text=line; pr.font.size=Pt(size); pr.font.bold=bold
        pr.font.name=font; pr.font.color.rgb=RGBColor.from_string(color)
    return sh

def slide(title, subtitle="", section="EXPERIMENT"):
    s=p.slides.add_slide(blank)
    s.background.fill.solid(); s.background.fill.fore_color.rgb=RGBColor.from_string(WHITE)
    box(s,0,0,13.33,.12,TEAL)
    text(s,section,.58,.34,12,.4,12,TEAL,True)
    text(s,title,.58,.91,12.1,.65,29,NAVY,True)
    text(s,subtitle,.60,1.59,12,.45,15,GRAY)
    text(s,"KAFKA SHARE GROUPS  /  NOV 2026",.6,7.07,9,.30,10,GRAY)
    return s

def card(s,heading,body,x,y,w,h=1.53,accent=TEAL):
    box(s,x,y,w,h)
    box(s,x,y,.10,h,accent)
    text(s,heading,x+.22,y+.17,w-.4,.38,19,accent,True)
    text(s,body,x+.22,y+.68,w-.4,h-.73,15,GRAY)

def metrics(s,items):
    for i,(value,title) in enumerate(items):
        x=.66+4.16*i
        box(s,x,2.50,3.82,1.55)
        text(s,value,x+.22,2.68,3.30,.68,32,TEAL,True)
        text(s,title,x+.22,3.50,3.42,.34,14,GRAY)

def code(title,subtitle,body,section):
    s=slide(title,subtitle,section)
    box(s,.60,2.05,12.14,4.77,NAVY)
    lines=body.strip().splitlines()
    text(s,"\n".join(lines),.84,2.30,11.6,4.42,
         12 if len(lines)<19 else 11,WHITE,False,"Consolas")
    return s

# 1
s=slide("Kafka Share Groups vs Classic Consumer Groups",
        "One partition. Two consumers. One deliberately slow worker.")
card(s,"The hypothesis","Can a fast worker make progress while the other is blocked?",.66,2.47,12.0,1.54)
card(s,"The variables","20 records  /  20s slow handler  /  100ms fast handler",.66,4.29,12.0,1.54,BLUE)

# 2
s=slide("How the experiment works","The Java ProcessingService is identical in both consumption modes.")
metrics(s,[("1","topic partition"),("20","record IDs 0-19"),("2","consumer instances")])
card(s,"Slow worker","Thread.sleep(20000)",.67,4.59,5.73,1.30)
card(s,"Fast worker","Thread.sleep(100)",6.85,4.59,5.73,1.30,BLUE)

# 3
code("Share Groups: the complete bean",
     "The share listener container creates a KafkaShareConsumer.",'''
@Bean
@ConditionalOnProperty(name = "poc.mode", havingValue = "share")
IntegrationFlow shareFlow(ProcessingService processor) {
    Map<String, Object> props = consumerProps("poc-share-group");
    props.put("share.acquire.mode", "record_limit");
    ContainerProperties cp = new ContainerProperties("poc.share");
    cp.setShareAckMode(ContainerProperties.ShareAckMode.EXPLICIT);
    cp.setShareAutoRenewLocks(autoRenewLocks);
    ShareKafkaMessageListenerContainer<String, String> container =
        new ShareKafkaMessageListenerContainer<>(
            new DefaultShareConsumerFactory<>(props), cp);
    container.setConcurrency(1);
    container.setClientId(workerId);
    return IntegrationFlow
        .from(Kafka.shareMessageDrivenChannelAdapter(container)
            .id("shareAdapter"))
        .handle(processor::process)
        .get();
}''',"SHARE  /  CODE")

# 4
s=slide("Share: two active workers","Record-level acquisition on the same Kafka partition.","SHARE  /  SCHEMA")
card(s,"Kafka topic","poc.share / partition 0 / messages 0-19",.66,2.42,12,1.47,BLUE)
text(s,"DOWN TO TWO SHARE CONSUMERS",3.56,4.03,7,.34,16,GRAY,True)
card(s,"SLOW worker","Record 0 takes 20 seconds.",.66,4.60,5.76,1.56)
card(s,"FAST worker","Records 1-19 finish earlier.",6.87,4.60,5.76,1.56)

# 5
s=slide("Share: observed results","Analyzer output from the local Docker PoC.","SHARE  /  RESULTS")
metrics(s,[("20.00s","first START to last DONE"),("2","concurrent handlers"),("19","ordering inversions")])
card(s,"Message distribution","Fast: 19 completed / Slow: 1 completed",.66,4.63,12,1.50)

# 6
s=slide("Share: completion ordering","The slow record 0 finishes after records 1-19.","SHARE  /  ORDER")
card(s,"Observed DONE order","1, 2, 3, 4, ... 17, 18, 19, 0",.66,2.46,12,1.50)
card(s,"SLOW","ID 0: approximately 20 seconds",.66,4.38,5.77,1.50)
card(s,"FAST","19 records: 100ms handler each",6.87,4.38,5.77,1.50,BLUE)

# 7
code("Classic Groups: the complete bean",
     "A traditional KafkaConsumer gets exclusive partition assignment.",'''
@Bean
@ConditionalOnProperty(name = "poc.mode", havingValue = "classic")
IntegrationFlow classicFlow(ProcessingService processor) {
    Map<String, Object> props = consumerProps("poc-classic-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return IntegrationFlow
        .from(Kafka.messageDrivenChannelAdapter(
            new DefaultKafkaConsumerFactory<String, String>(props),
            KafkaMessageDrivenChannelAdapter.ListenerMode.record,
            "poc.classic")
            .configureListenerContainer(c -> c
                .id("classicContainer")
                .concurrency(1)))
        .handle(processor::process)
        .get();
}''',"CLASSIC  /  CODE")

# 8
s=slide("Classic: one partition owner at a time",
        "The fast and slow workers belong to one classic consumer group.","CLASSIC  /  SCHEMA")
card(s,"Kafka topic","poc.classic / one partition / messages 0-19",.66,2.45,12,1.55,BLUE)
card(s,"Exclusive assignment","One worker at a time processes this single partition.",.66,4.50,12,1.54,BLUE)
text(s,"A rebalance can move ownership from slow to fast during the run.",.83,6.32,11.3,.43,16,GRAY)

# 9
s=slide("Classic: observed results","Analyzer output from the local Docker PoC.","CLASSIC  /  RESULTS")
metrics(s,[("22.05s","first START to last DONE"),("1","concurrent handler"),("0","ordering inversions")])
card(s,"Message distribution","Fast: 19 completed / Slow: 1 completed",.66,4.63,12,1.50,BLUE)

# 10
s=slide("Classic: completion ordering",
        "All 20 record IDs completed sequentially in this run.","CLASSIC  /  ORDER")
card(s,"Observed DONE order","0, 1, 2, 3, 4, 5, ... 17, 18, 19",.66,2.46,12,1.50,BLUE)
card(s,"SLOW first","ID 0 takes approximately 20 seconds.",.66,4.38,5.77,1.50,BLUE)
card(s,"FAST afterward","Records 1-19 take about 1.9s in total.",6.87,4.38,5.77,1.50,BLUE)

# 11
s=slide("Share vs Classic: results compared",
        "Same messages, same processing service, same slow and fast delays.","COMPARISON")
entries=[("Messages completed","20","20"),("Fast / slow worker","19 / 1","19 / 1"),
         ("Concurrent handlers","2","1"),("Ordering inversions","19","0"),
         ("Elapsed time","20.00s","22.05s"),("Completion rate","1.00 msg/s","0.91 msg/s")]
text(s,"METRIC",.95,2.22,5,.42,16,TEAL,True)
text(s,"SHARE",7.40,2.22,2.1,.42,16,TEAL,True)
text(s,"CLASSIC",10.1,2.22,2.1,.42,16,BLUE,True)
for i,(name,shared,classic) in enumerate(entries):
    y=2.78+i*.54
    box(s,.73,y,11.9,.5,PALE if i%2==0 else WHITE)
    text(s,name,.95,y+.08,5.8,.35,15,NAVY)
    text(s,shared,7.40,y+.08,2.2,.35,15,TEAL,True)
    text(s,classic,10.1,y+.08,2.3,.35,15,BLUE,True)

# 12
s=slide("What the experiment really proves",
        "Observed data is evidence, not a universal benchmark.","TAKEAWAYS")
card(s,"Supported observations","Share: concurrent processing on a single partition.\nClassic: one active partition owner.\nShare: completion order differed from record ID order.",.66,2.32,12,2.10)
card(s,"Still to test","Repeated runs, larger batches, lock expiry/renewal,\nsteady-state throughput and duplicate handling.",.66,4.77,12,1.52,BLUE)

target=Path(__file__).with_name("kafka_share_groups_office2016.pptx")
p.save(target)
print(f"Generated {target} ({len(p.slides)} slides)")
