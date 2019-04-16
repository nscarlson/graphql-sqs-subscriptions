import { Consumer as SqsSubscriber } from 'sqs-consumer'
import { SQS as SqsPublisher } from 'aws-sdk'
import { PubSubEngine } from 'graphql-subscriptions'
import { PubSubAsyncIterator } from './pubsub-async-iterator'

export interface SqsSubscriptionOptions {
    queueUrl?: string
    handleMessage?: Function
    publisher?: SqsPublisher
    subscriber?: SqsSubscriber
    reviver?: Reviver
}

export class SqsPubSub implements PubSubEngine {
    constructor(options: SqsSubscriptionOptions = {}) {
        const { queueUrl, subscriber, publisher, reviver } = options

        this.reviver = reviver

        if (subscriber && publisher) {
            this.sqsPublisher = publisher
            this.sqsSubscriber = subscriber
        } else {
            try {
                this.sqsPublisher = new SqsPublisher({
                    apiVersion: '2012-11-05',
                    region: 'us-east-2',
                })

                this.sqsSubscriber = SqsSubscriber.create({
                    queueUrl,
                    handleMessage: this.onMessage.bind(this),
                })
                    .on('error', (err) => {
                        console.error({ err: err.message })
                    })
                    .on('processing_error', (err) => {
                        console.error({ err: err.message })
                    })
            } catch (error) {
                console.error(`Error connecting to SQS queue.`)
            }
        }

        // handle messages received via subscribe
        this.sqsSubscriber.on('pmessage', this.onMessage.bind(this))
        // partially applied function passes undefined for pattern arg since 'message' event won't provide it:
        this.sqsSubscriber.on('message', this.onMessage.bind(this, undefined))

        this.subscriptionMap = {}
        this.subsRefsMap = {}
        this.currentSubscriptionId = 0
    }

    public async publish(queueUrl: string, payload: any): Promise<void> {
        await this.sqsPublisher
            .sendMessage({
                MessageBody: JSON.stringify(payload),
                QueueUrl: queueUrl,
                MessageAttributes: {
                    data: {
                        DataType: 'String',
                        StringValue: 'test',
                    },
                },
            })
            .promise()
    }

    public subscribe(queueUrl: string, onMessage: Function): Promise<number> {
        const id = this.currentSubscriptionId++
        this.subscriptionMap[id] = [queueUrl, onMessage]

        const refs = this.subsRefsMap[queueUrl]

        if (refs && refs.length > 0) {
            const newRefs = [...refs, id]
            this.subsRefsMap[queueUrl] = newRefs
            return Promise.resolve(id)
        } else {
            return new Promise<number>((resolve, reject) => {
                this.subsRefsMap[queueUrl] = [
                    ...(this.subsRefsMap[queueUrl] || []),
                    id,
                ]
                resolve(id)
            })
        }
    }

    public unsubscribe(subId: number) {
        const [triggerName = null] = this.subscriptionMap[subId] || []
        const refs = this.subsRefsMap[triggerName]

        if (!refs) throw new Error(`There is no subscription of id "${subId}"`)

        if (refs.length === 1) {
            // unsubscribe from specific channel and pattern match
            this.sqsSubscriber.stop()

            delete this.subsRefsMap[triggerName]
        } else {
            const index = refs.indexOf(subId)
            const newRefs =
                index === -1
                    ? refs
                    : [...refs.slice(0, index), ...refs.slice(index + 1)]
            this.subsRefsMap[triggerName] = newRefs
        }
        delete this.subscriptionMap[subId]
    }

    public asyncIterator<T>(
        triggers: string | string[],
        options?: Object,
    ): AsyncIterator<T> {
        return new PubSubAsyncIterator<T>(this, triggers, options)
    }

    public close(): void {
        // this.sqsPublisher.quit()
        this.sqsSubscriber.stop()
    }

    private onMessage(queueUrl: string, message: string) {
        const subscribers = this.subsRefsMap[queueUrl]

        // Don't work for nothing..
        if (!subscribers || !subscribers.length) {
            return
        }

        let parsedMessage
        try {
            parsedMessage = JSON.parse(message, this.reviver)
        } catch (e) {
            parsedMessage = message
        }

        for (const subId of subscribers) {
            const [, listener] = this.subscriptionMap[subId]
            listener(parsedMessage)
        }
    }

    private sqsSubscriber: SqsSubscriber
    private sqsPublisher: SqsPublisher
    private reviver: Reviver

    private subscriptionMap: { [subId: number]: [string, Function] }
    private subsRefsMap: { [trigger: string]: Array<number> }
    private currentSubscriptionId: number
}

export type Path = Array<string | number>
export type Trigger = string | Path
export type TriggerTransform = (
    trigger: Trigger,
    channelOptions?: Object,
) => string
export type Reviver = (key: any, value: any) => any
